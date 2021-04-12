# Pipeline

功能：

- 支持根据模型批量创建数据库表
- 支持自定义任务流程图
- 支持基于 kahn 算法的 DAG 检测
- 支持执行 bash 脚本
- 支持自定义脚本参数，当流程走到某个顶点时，自动读填充参数并执行脚本
- 支持自动跳转到下一个顶点
- 支持查看 pipeline 历史轨迹
- 支持 WEB 图形展示

# 效果截图

# 数据库模型

# 使用示例

## pipeline 相关内键 API

```python
from pipeline.model import db
from pipeline.service import create_graph, add_vertex, add_edge, check_graph_all

db.drop_all()  # 清空数据库  
db.create_all()  # 按照 model 创建表
check_graph_all()  # 检查每个流程是否符合 DAG
import json

# 创建 DAG 示例
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
```

## 启动一条 pipeline

详细见：`pipeline.app`

## 启动 WEB 界面

```python
from web import web

# 注意，数据展示需要数据库中先有数据才行
if __name__ == '__main__':
    web.run(host='0.0.0.0', port=5000)
```