from pipeline.model import Graph, Vertex, Edge, Pipeline, Track, db
from pipeline.model import STATE_WAITING, STATE_PENDING, STATE_RUNNING, STATE_SUCCEED, STATE_FAILED, STATE_FINISH
import re
from pipeline.service import transactional
import simplejson
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import queue
from subprocess import Popen
from tempfile import TemporaryFile
from loguru import logger
from collections import defaultdict


@transactional
def start_pipeline(g_id, name: str, desc: str = None):
    """ 查询入度为零的点 的 SQL """

    graph = db.session.query(Graph).get(g_id)
    if not graph.checked:
        raise ValueError('这个图不符合 DAG')

    # 写入 pipeline 表
    p = Pipeline()
    p.name = name
    p.graph_id = graph.id
    p.state = STATE_RUNNING
    p.desc = desc
    db.session.add(p)

    # 查询这个 graph 中所有顶点
    vertexes = db.session.query(Vertex).filter(Vertex.graph_id == graph.id)

    # 获取入度为 0 的点    这样使用子查询只需要去数据库查一次
    zds = vertexes.filter(Vertex.id.notin_(db.session.query(Edge.head).filter(Edge.graph_id == graph.id))).all()
    # 等价 SQL：子查询的方式
    # SELECT * FROM vertex WHERE graph_id =1 AND vertex.id NOT IN (SELECT edge.head from edge WHERE graph_id =1)

    for v in vertexes:
        # 写入 track 表
        t = Track()

        # 由于 pipeline 还未提交，并没有 id，而且这时也不想提交，那么 t.pipeline_id 应该等于什么？怎么办？
        # 因为在 model 中定义了外键，所以使用 t.pipeline 让 SQLALchemy 自己通过 relationship 找映射关系
        t.pipeline = p  # 等价 t.pipeline_id = p.id
        t.vertex_id = v.id
        t.state = STATE_PENDING if v in zds else STATE_WAITING
        # t.input = v.input
        # t.script = v.script
        db.session.add(t)

    # 标记有人使用过了，sealed 封闭
    if graph.sealed == 0:
        graph.sealed = 1
        db.session.add(graph)

    return p


def show_pipeline(p_id, state=[STATE_PENDING], exclude=[STATE_FAILED]):
    """
    显示流程相关的所有信息：流程信息、顶点状态、顶点里边的 input 和 script
    """

    # 这样会查多次数据库，效率极差
    # query = db.session.query(Track).filter(Track.pipeline_id == p_id).filter(Track.state.in_(state))
    # pipelines = []
    # for track in query:
    #     pipelines.append((track.pipeline.id, track.pipeline.name, track.pipeline.state,
    #                       track.id, track.state,
    #                       track.vertex.id, track.vertex.name, track.vertex.input, track.vertex.script))

    # print(pipelines)

    # 使用 join 方式，只查一次数据库
    pipelines = db.session.query(Pipeline.id, Pipeline.name, Pipeline.state,
                                 Track.id, Track.state,
                                 Vertex.id, Vertex.name, Vertex.input, Vertex.script) \
        .join(Track, Track.pipeline_id == Pipeline.id) \
        .join(Vertex, Vertex.id == Track.vertex_id) \
        .filter(Track.pipeline_id == p_id) \
        .filter(Track.state.in_(state)) \
        .filter(Pipeline.state != exclude)

    return pipelines.all()


TYPES = {
    'str': str,
    'string': str,
    'int': int,
    'integer': int
}

"""
input = {"ip": {"type": "str", "required": True, "default": '192.168.0.100'}}
"""


def finish_params(t_id, d: dict, inp):
    """ 把参数值转换为指定的类型"""

    # inp  {'ip': {'type': 'str', 'required': True, 'default': '192.168.0.100'}}
    # d    {'ip': '192.168.0.100'}

    if inp:
        params = {}
        for k, v in inp.items():  # 遍历 inp
            if k in d.keys():  # 如果 k 在 d 中也有对应，就。。
                params[k] = TYPES.get(v['type'], str)(d[k])  # 用 inp 中指定的类型参数，去转换 d 中对应的值的类型。
            elif v.get('default', False):  # 如果 k 没有对应的上，就去转换 default 中的值
                params[k] = TYPES.get(v['type'], str)(v.get('default'))
            else:
                raise TypeError('参数类型错误')
        # params: {'ip': '192.168.0.100'}
        return params


@transactional
def finish_script(t_id, script: str, params: dict):
    """
    把参数填充进 script 内
    之后把 script 和 input 存 track 库
    """

    # script: {'script': 'echo "test1.A"\nping {ip}', 'next': 'B'}

    try:
        if script:
            if not isinstance(script, str):
                raise
            script = simplejson.loads(script).get('script')
    except Exception as e:
        # logger.error(e)
        script = ''

    newline = ''
    start = 0
    pattern = re.compile(r'{([^{}]+)}')
    for matcher in pattern.finditer(script):  # 把 script 中留的关键字取出来
        newline += script[start:matcher.start()]  # 拼字
        tmp = params.get(matcher.group(1), '')  # 用关键字去参数 params 中找出对应的值
        newline += str(tmp)  # 拼字
        start = matcher.end()  # 拼字
    else:
        newline += script[start:]  # 拼字

    # newline:      echo "test1.A"\nping 192.168.0.100
    track = db.session.query(Track).filter(Track.id == t_id).one()
    track.input = simplejson.dumps(params)
    track.script = newline
    db.session.add(track)

    return newline


class Executor:
    def __init__(self):
        self._executor = ThreadPoolExecutor(max_workers=3)  # 池最大运行 3 个线程
        self._tasks = {}
        self._event = threading.Event()
        self._queue = queue.Queue()
        self._states = defaultdict(dict)
        threading.Thread(target=self._run).start()
        threading.Thread(target=self._save_track).start()

    def _script_executor(self, track_id, script):
        code_state = 0
        with TemporaryFile('a+') as tf:  # 打开临时文件
            for line in script.splitlines():  # 按行分割
                p = Popen(line, shell=True, stdout=tf, stderr=tf)
                code = p.wait()  # 阻塞等，code 为 0 是正确执行
                code_state = code_state | code  # 总状态码，有一次运行失败即为失败
            tf.flush()  # flush 文件到磁盘
            tf.seek(0)  # 游标回到开头
            texts = tf.read()  # 读取执行结果

        return code_state, texts

    def executor(self, track_id):
        """异步执行方法，只负责提交任务，任务运行结束，会返回运行结果"""
        try:
            # 修改 track 状态为 RUNNING
            track = db.session.query(Track).get(track_id)
            track.state = STATE_RUNNING
            db.session.add(track)
            db.session.commit()
            self._tasks[self._executor.submit(self._script_executor, track.id, track.script)] = track.id  # 异步提交任务
        except Exception as e:
            logger.error(e)
            db.session.rollback()

    def _run(self):
        """单独一个线程，等待任务的返回值"""
        while not self._event.wait(3):
            for future in as_completed(self._tasks):  # 阻塞在这等返回值
                track_id = self._tasks[future]
                try:
                    code_state, texts = future.result()
                    self._queue.put((track_id, code_state, texts))
                except Exception as e:
                    logger.error(e)
                finally:
                    del self._tasks[future]  # 任务运行完要从任务列表中删除

    def _save_track(self):
        while not self._event.wait(3):
            track_id, code_state, texts = self._queue.get()  # 阻塞在这等
            track = db.session.query(Track).get(track_id)
            track.output = texts
            track.state = STATE_SUCCEED if code_state == 0 else STATE_FAILED  # 判断 track 运行状态

            if code_state != 0:  # 如果失败，必须立即将任务流状态设置也为失败
                logger.info(f'=============== {track.vertex_id} 运行失败')
                track.pipeline.state = STATE_FAILED
            else:
                logger.info(f'=============== {track.vertex_id} 运行成功')
                others = db.session.query(Track).filter(
                    (Track.pipeline_id == track.pipeline_id) & (Track.id != track.id)).all()

                states = {STATE_WAITING: 0, STATE_PENDING: 0, STATE_RUNNING: 0, STATE_SUCCEED: 0, STATE_FAILED: 0}

                for t in others:
                    states[t.state] += 1

                if states[STATE_FAILED] > 0:  # 任务流中有错误
                    logger.info('任务流中有错误')
                    track.pipeline.state = STATE_FAILED
                elif len(others) == states[STATE_SUCCEED]:  # 自己是最后终点，其他全部成功
                    logger.info(f'{track.vertex_id} 是最后终点')
                    track.pipeline.state = STATE_FINISH
                else:
                    edges = db.session.query(Edge).filter(Edge.graph_id == track.pipeline.graph_id).all()
                    tailTOhead = defaultdict(list)
                    headTOtail = defaultdict(list)
                    for edge in edges:
                        tailTOhead[edge.tail].append(edge.head)
                        headTOtail[edge.head].append(edge.tail)

                    if track.vertex_id in tailTOhead.keys():  # 如果在，说明后边还有其他弧尾
                        logger.info(f'还有其他节点{tailTOhead[track.vertex_id]}')
                        nexts = tailTOhead[track.vertex_id]  # 以弧头，找弧尾集合
                        for n in nexts:
                            tails = headTOtail[n]  # 用弧尾，找到对应的弧头集合。
                            query = db.session.query(Track).filter(  # 查对应弧头们的执行状态
                                (Track.pipeline_id == track.pipeline_id) & (Track.vertex_id.in_(tails)) & (
                                        Track.state == STATE_SUCCEED)).count()
                            if query == len(tails):  # 如果都成功了，那么这个弧尾就可以 pending 了,前置全部做完
                                nx = db.session.query(Track).filter(Track.vertex_id == n).one()
                                nx.state = STATE_PENDING
                                db.session.add(nx)
                                logger.info(f'{n} 的前置全部做完，改为 pending')
                            else:  # 还有前置没有做完
                                self.info = logger.info(f'{n} 还有前置还未全部执行')
                                pass  # 什么都不做
            try:
                db.session.commit()
            except Exception as e:
                logger.error(e)
                db.session.rollback()


# 全局的任务执行器对象
executor = Executor()
