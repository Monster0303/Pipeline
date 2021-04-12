from pipeline.model import db, Graph, Vertex, Edge
from loguru import logger
from functools import wraps
import copy
from collections import defaultdict


def transactional(fn):
    """负责提交事务的装饰器"""

    @wraps(fn)
    def _wraper(*args, **kwargs):
        ret = fn(*args, **kwargs)
        try:
            db.session.commit()
            return ret
        except Exception as error:
            logger.error(error)
            db.session.rollback()

    return _wraper


@transactional
def create_graph(name, desc=None):
    """ 创建符合 DAG 的图 """
    g = Graph()
    g.name = name
    g.desc = desc

    db.session.add(g)
    return g


@transactional
def add_vertex(graph: Graph, name, input=None, script=None):
    """ 增加顶点 """
    v = Vertex()
    v.name = name
    v.script = script
    v.input = input
    v.graph_id = graph.id

    db.session.add(v)
    return v


@transactional
def add_edge(graph: Graph, tail: Vertex, head: Vertex):
    """ 增加边 """
    e = Edge()
    e.graph_id = graph.id
    e.tail = tail.id
    e.head = head.id

    db.session.add(e)
    return e


@transactional
def del_vertex(v_id):
    """ 删除顶点，和所有与顶点关联的边 """
    v = db.session.query(Vertex).get(v_id)
    if v:  # 找到顶点后，删除关联的边，然后删除顶点
        db.session.query(Edge).filter((Edge.tail == v.id) | (Edge.head == v.id)).delete()  # 删除边
        db.session.delete(v)  # 删除顶点
    return v


"""
##### 提取出指定 graph 中入度为 0 的顶点的 SQL 语句
左 join 方式的 SQL：
SELECT *
from vertex v LEFT join edge e
ON v.graph_id = e.graph_id AND e.head = v.id
WHERE v.graph_id = 1 AND e.head IS null

子查询的方式 SQL：
SELECT * FROM vertex WHERE graph_id =1 AND vertex.id NOT IN (SELECT edge.head from edge WHERE graph_id =1)

"""


# kahn 算法实现(卡恩)
# 效率低，太多迭代，建议使用下面经过优化的
def check_graph(graph: Graph) -> bool:
    query = db.session.query(Vertex).filter(Vertex.graph_id == graph.id)
    vertexes = [x for x in query]
    query = db.session.query(Edge).filter(Edge.graph_id == graph.id)
    edges = [x for x in query]

    if not edges or not vertexes:
        return False

    while vertexes:
        v_list = copy.copy(vertexes)

        for e in edges:
            for v in v_list:
                if e.head == v.id:
                    v_list.remove(v)
        else:
            if not v_list:
                return False

        e_list = []
        for v in v_list:
            for e in edges:
                if e.tail == v.id:
                    e_list.append(e)

        for e in e_list:
            edges.remove(e)
        for v in v_list:
            vertexes.remove(v)
    else:
        if len(vertexes) + len(edges) == 0:
            return True
        else:
            return False


# kahn 算法实现(卡恩)优化后
def check_graph(graph: Graph) -> bool:
    query = db.session.query(Vertex).filter(Vertex.graph_id == graph.id)
    vertexes = {v.id for v in query}
    query = db.session.query(Edge).filter(Edge.graph_id == graph.id)
    edges = defaultdict(list)

    if not vertexes:  # 如果没有顶点，直接错误
        return False

    ids = set()  # 有入度的顶点
    for edge in query:
        edges[edge.tail].append((edge.tail, edge.head))
        ids.add(edge.head)
    # 生成边的集合，以起始的顶点为 key  { 1: [(1, 2), (1, 3)], 2: [(2, 4)], 3: [(3, 2)] }

    if len(edges) == 0:  # 如果没有边，虽然也是 DAG，但是业务上不用
        return False

    # 如果 edges 不为空，一定有 ids，也就是有入度的顶点一定会有
    while edges:
        zds = vertexes - ids  # 没有入度的顶点 = 全部点 - 有入度的点

        if len(zds):  # zds 为 0 说明没有找到入度为 0 的顶点，算法终止
            for zd in zds:
                if zd in edges:  # 有可能顶点没有出度
                    del edges[zd]
        else:
            break

        vertexes = ids
        ids = set()  # 重新寻找有入度的顶点

        for lst in edges.values():
            for edge in lst:
                ids.add(edge[1])

    # 如果边集为空，剩下所有顶点都是入度为 0 的，没必要在多次迭代删除顶点
    if len(edges) == 0:

        # 检验通过，修改checked字段为1
        try:
            graph = db.session.query(Graph).get(graph.id)
            if graph:
                graph.checked = 1
                db.session.add(graph)
                db.session.commit()
                return True
        except Exception as e:
            logger.error(e)
            db.session.rollback()
            raise e

    return False


# 检查所有 checked 字段为 0 的 Graph 是否符合 DAG
def check_graph_all():
    query = db.session.query(Graph).filter(Graph.checked == 0)
    for graph in query:
        if check_graph(graph):
            logger.info(f'check dag done: {graph.name} yes dag')
        else:
            logger.info(f'check dag done: {graph.name} is not dag')
