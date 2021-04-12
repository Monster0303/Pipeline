# Pipeline

Pipeline 功能：

- 支持根据模型批量创建数据库表
- 支持自定义任务流程图
- 支持基于 kahn 算法的 DAG 检测
- 支持执行 bash 脚本
- 支持自定义脚本参数，当流程走到某个顶点时，自动填充参数并执行脚本
- 支持自动跳转到下一个顶点
- 支持查看 pipeline 历史轨迹

支持 WEB 图形展示:

- 使用 Flask 微框架
- 使用 Jinja2 模板技术
- 使用 JQuery 发起 AJAX 异步调用
- 使用 Apache ECharts 图表组件（百度开源，Apache 孵化）

# 效果截图

![show_1](https://user-images.githubusercontent.com/40815364/114376638-64102700-9bb8-11eb-80f8-53e913e4f12d.png)
![show_2](https://user-images.githubusercontent.com/40815364/114376689-74c09d00-9bb8-11eb-87d2-1a63908c4e8e.png)
![show_3](https://user-images.githubusercontent.com/40815364/114376697-768a6080-9bb8-11eb-9c2e-75eb47c84809.png)
![show_4](https://user-images.githubusercontent.com/40815364/114376713-78ecba80-9bb8-11eb-9bea-17a174350a92.png)

# 数据库模型

![database](https://user-images.githubusercontent.com/40815364/114376363-24493f80-9bb8-11eb-9c94-853d042ddfc2.png)

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

详细见：[pipeline.app](https://github.com/Monster0303/Pipeline/blob/main/pipeline/app.py)

## 启动 WEB 界面

```python
from web import web

# 注意，数据展示需要数据库中先有数据才行
if __name__ == '__main__':
    web.run(host='0.0.0.0', port=5000)
```
