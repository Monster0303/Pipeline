import json
from pipeline.model import db
from loguru import logger
from pipeline.service import create_graph, add_vertex, add_edge, check_graph_all
import simplejson
from pipeline.executor import start_pipeline, show_pipeline, finish_params, finish_script, executor
import time


# 测试数据
def create_test_dag():
    try:
        # 创建DAG
        g = create_graph('test1')  # 成功则返回一个 Graph 对象
        # 增加顶点
        input = {"ip": {"type": "str", "required": True, "default": '127.0.0.1'}}
        script = {'script': 'echo "test1.A"\nping {ip} -c 2'}
        # 这里为了让用户方便，next 可以接收2种类型，数字表示顶点的id，字符串表示同一个DAG中该名称的节点， 不能重复
        a = add_vertex(g, 'A', json.dumps(input), json.dumps(script))  # next 顶点验证可以在定义时，也可以在使用时
        b = add_vertex(g, 'B', None, '{"script":"echo B"}')
        c = add_vertex(g, 'C', None, '{"script":"echo C"}')
        d = add_vertex(g, 'D', None, '{"script":"echo D"}')
        # 增加边
        ab = add_edge(g, a, b)
        ac = add_edge(g, a, c)
        cb = add_edge(g, c, b)
        bd = add_edge(g, b, d)

        # 创建DAG环路
        g = create_graph('test2')  # 环路
        # 增加顶点
        a = add_vertex(g, 'A', None, '{"script":"echo A"}')
        b = add_vertex(g, 'B', None, '{"script":"echo B"}')
        c = add_vertex(g, 'C', None, '{"script":"echo C"}')
        d = add_vertex(g, 'D', None, '{"script":"echo D"}')
        # 增加边, abc之间的环
        ba = add_edge(g, b, a)
        ac = add_edge(g, a, c)
        cb = add_edge(g, c, b)
        bd = add_edge(g, b, d)

        # 创建DAG多个终点
        g = create_graph('test3')  # 多个终点
        # 增加顶点
        a = add_vertex(g, 'A', None, '{"script":"echo A"}')
        b = add_vertex(g, 'B', None, '{"script":"echo B"}')
        c = add_vertex(g, 'C', None, '{"script":"echo C"}')
        d = add_vertex(g, 'D', None, '{"script":"echo D"}')

        # 增加边
        ba = add_edge(g, b, a)
        ac = add_edge(g, a, c)
        bc = add_edge(g, b, c)
        bd = add_edge(g, b, d)

        # 创建DAG多入口
        g = create_graph('test4')  # 多入口
        # 增加顶点
        a = add_vertex(g, 'A', None, '{"script":"echo A"}')
        b = add_vertex(g, 'B', None, '{"script":"echo B"}')
        c = add_vertex(g, 'C', None, '{"script":"echo C"}')
        d = add_vertex(g, 'D', None, '{"script":"echo D"}')
        # 增加边
        ab = add_edge(g, a, b)
        ac = add_edge(g, a, c)
        cb = add_edge(g, c, b)
        db = add_edge(g, d, b)

        # 创建DAG没有边
        g = create_graph('test5')  # 没有边
        # 增加顶点
        a = add_vertex(g, 'A', None, '{"script":"echo A"}')
        b = add_vertex(g, 'B', None, '{"script":"echo B"}')
        c = add_vertex(g, 'C', None, '{"script":"echo C"}')
        d = add_vertex(g, 'D', None, '{"script":"echo D"}')
    except Exception as e:
        logger.error(e)


db.drop_all()
db.create_all()
create_test_dag()  # 创建上边的测试数据
check_graph_all()  # 检查每个流程是否符合 DAG

# 执行一条 Pipeline，并指定使用哪个 Graph，测试数据中有 1-5 号
start_pipeline(4, "测试1", "这是测试1")

while True:
    # 找某个 pipeline 中指定状态的 track
    # 可用状态有：
    #       STATE_WAITING = 0
    #       STATE_PENDING = 1
    #       STATE_RUNNING = 2
    #       STATE_SUCCEED = 3
    #       STATE_FAILED = 4
    #       STATE_FINISH = 5
    ps = show_pipeline(1, [0,1,2,3,4,5])  # pipeline.id, track；
    d = {}

    for p in ps:  # 可能多个 track
        pipeline_id, pipeline_name, pipeline_state, track_id, track_state, vertex_id, vertex_name, vertex_input, vertex_script = p
        if vertex_input:
            try:
                inp = simplejson.loads(vertex_input)
                if not isinstance(inp, dict):
                    inp = {}
            except:
                raise

            # input     {'ip': {'type': 'str', 'required': True, 'default': '192.168.0.100'}}
            # script    {'script': 'echo "test1.A"\nping {ip} -c 2', 'next': 'B'}
            for k, v in inp.items():
                if v.get('required!!!', False) is True:
                    params = input(f'{k} = ')
                else:
                    params = v.get('default', '127.0.0.1')
                d[k] = params
        else:
            inp = {}
            d = {}

        # d {'ip': '192.168.0.100'}
        # 准备好参数       返回值 params: {'ip': '192.168.0.100'}
        params = finish_params(track_id, d, inp)
        # 生成替换好的脚本，同时存到 track 库     返回值  script:  echo "test1.A"\nping 192.168.0.100
        script = finish_script(track_id, vertex_script, params)

        executor.executor(track_id)
    time.sleep(2)
