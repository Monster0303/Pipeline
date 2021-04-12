# coding: utf-8
from sqlalchemy import Column, ForeignKey, String, Text, create_engine
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from pipeline import config
from functools import wraps

"""
这里的 model 是为 WEB 项目的 service 提供数据库支持的，和 pipeline 中的文件一模一样
如果使用同一个 model 文件，WEB 项目和 Pipeline 项目最好分开使用连接和 session
这里直接使用了两个
"""

Base = declarative_base()
metadata = Base.metadata

STATE_WAITING = 0
STATE_PENDING = 1
STATE_RUNNING = 2
STATE_SUCCEED = 3
STATE_FAILED = 4
STATE_FINISH = 5


class Graph(Base):
    __tablename__ = 'graph'

    id = Column(INTEGER(11), primary_key=True)
    name = Column(String(45), nullable=False)
    desc = Column(String(100))
    checked = Column(INTEGER(11), nullable=False, default=0)
    sealed = Column(INTEGER(11), nullable=False, default=0)

    # 经常从图查看所有顶点、边的信息
    vertexes = relationship('Vertex')
    edges = relationship('Edge')


class Vertex(Base):
    __tablename__ = 'vertex'

    id = Column(INTEGER(11), primary_key=True)
    name = Column(String(45), nullable=False)
    graph_id = Column(ForeignKey('graph.id'), nullable=False, index=True)
    script = Column(Text)
    input = Column(Text)

    graph = relationship('Graph')

    # 从顶点查它的边，这里必须使用 foreign_keys，其值必须使用引号
    tails = relationship('Edge', foreign_keys='[Edge.tail]')
    heads = relationship('Edge', foreign_keys='Edge.head')


class Edge(Base):
    __tablename__ = 'edge'

    id = Column(INTEGER(11), primary_key=True)
    tail = Column(ForeignKey('vertex.id'), nullable=False, index=True)
    head = Column(ForeignKey('vertex.id'), nullable=False, index=True)
    graph_id = Column(ForeignKey('graph.id'), nullable=False, index=True)

    graph = relationship('Graph')
    vertex = relationship('Vertex', primaryjoin='Edge.head == Vertex.id')
    vertex1 = relationship('Vertex', primaryjoin='Edge.tail == Vertex.id')


class Pipeline(Base):
    __tablename__ = 'pipeline'

    id = Column(INTEGER(11), primary_key=True)
    graph_id = Column(ForeignKey('graph.id'), nullable=False, index=True)
    name = Column(String(45), nullable=False)
    state = Column(INTEGER(11), nullable=False)
    desc = Column(String(100))

    tracks = relationship('Track', foreign_keys='Track.pipeline_id')


class Track(Base):
    __tablename__ = 'track'

    id = Column(INTEGER(11), primary_key=True)
    pipeline_id = Column(ForeignKey('pipeline.id'), nullable=False, index=True)
    vertex_id = Column(ForeignKey('vertex.id'), nullable=False, index=True)
    state = Column(INTEGER(11), nullable=False, default=STATE_WAITING)
    input = Column(Text)
    script = Column(Text)
    output = Column(Text)

    vertex = relationship('Vertex')
    pipeline = relationship('Pipeline')


def singleton(cls):
    """ 单实例化 """
    instance = None

    @wraps(cls)
    def get_instance(*args, **kwargs):
        nonlocal instance
        if not instance:
            instance = cls(*args, **kwargs)
        return instance

    return get_instance


@singleton
class Database:
    def __init__(self, url, **kwargs):
        #  Session 类和 engine 都是线程安全的，有一个就行了。所以使用单例化
        self._engine = create_engine(url, **kwargs)
        self._session = sessionmaker(bind=self._engine)()  # 在这里把 session 实例化，但是这样在多线程下是不安全的

    @property
    def session(self):
        return self._session

    @property
    def engine(self):
        return self._engine

    def drop_all(self):
        # 删除继承自 Base 的所有表
        Base.metadata.drop_all(self._engine)

    def create_all(self):
        # 创建继承自 Base 的所有表
        Base.metadata.create_all(self._engine)


# 模块加载一次，db也是单例的
db = Database(config.URL, echo=config.DATABASE_DEBUG)
