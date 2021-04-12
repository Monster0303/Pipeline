from .model import db, Pipeline, Track, Vertex, Edge
import random


# db = getdb() # 如果使用同一个 model 文件，WEB 项目和 Pipeline 项目最好分开使用连接和 session

def randomxy():
    # 随机模仿 x y坐标
    return random.randint(300, 500)


def getdag(pipeline_id):  # 根据 pipeline 的 id 返回流程数据，让前端页面绘制 DAG 图

    ps = db.session.query(Pipeline.id, Pipeline.name, Pipeline.state, Vertex.id, Vertex.name, Vertex.script,
                          Track.script).join(Track, (Pipeline.id == Track.pipeline_id) & (Pipeline.id == 1)) \
        .join(Vertex, Vertex.id == Track.vertex_id)

    edges = db.session.query(Edge.tail, Edge.head). \
        join(Pipeline, (Pipeline.graph_id == Edge.graph_id) & (Pipeline.id == 1))

    print([i for i in ps])
    print([i for i in edges])

    data = []  # 顶点数据
    vertexes = {}  # 让 edges 查询少 join
    title = ''
    for pipeline_id, p_name, p_state, vertex_id, v_name, v_script, t_script in ps:
        if not title:
            title = p_name
        data.append({
            'name': v_name,
            'x': randomxy(),
            'y': randomxy(),
            'value': t_script if t_script else v_script
        })
        vertexes[vertex_id] = v_name
    print(data)

    links = []  # 边
    for tail, head in edges:
        print(tail, head)
        print(vertexes)
        links.append({
            'source': vertexes[tail],
            'target': vertexes[head]
        })

    return {'title': title, 'data': data, 'links': links}
