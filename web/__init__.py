from flask import Flask, make_response, render_template, jsonify
from .service import getdag

web = Flask('pipeline_web')


@web.route('/', methods=['GET'])  # 路由，可以指定方法列表，缺省GET
def index():  # 视图函数
    return render_template('index.html')


@web.route('/<int:graphid>')  # index.html 中访问不同的模板页
def showdag(graphid):
    return render_template(f'chart{graphid}.html')


@web.route('/dag/<int:graphid>')  # (rule, **options):
def showajaxdag(graphid):
    if graphid == 1:
        return simplegraph()
    elif graphid == 2:
        return jsonify(getdag(1))
    elif graphid == 3:
        return jsonify(getdag(3))


def simplegraph():
    xs = ["衬衫", "羊毛衫", "雪纺衫", "裤子", "高跟鞋", "袜子"]
    data = [5, 20, 36, 10, 10, 20]
    return jsonify({'xs': xs, 'data': data})
