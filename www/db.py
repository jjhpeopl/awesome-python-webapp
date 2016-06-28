# _*_ coding: utf-8 _*_
__author__ = 'jjh'

import threading
import logging
import functools
import mysql.connector

engine = None


class DBError(Exception):
    pass

# 连接数据库
def create_engine(user, password, database, host='127.0.0.1', port=3306, **kw):
    """
        db模型的核心函数，用于连接数据库, 生成全局对象engine，
        engine对象持有数据库连接
    """

    # 引入数据库模块
    global engine

    if engine is not None:
        raise DBError('Engine is already initialized')

    # 封装参数
    params = dict(user=user, password=password, database=database, host=host, port=port)
    defaults = dict(use_unicode=True, charset='utf8', collation='utf8_general_ci', autocommit=False)

    # 从kw参数中获取设置的值
    for k, v in defaults.iteritems():
        params[k] = kw.pop(k, v)

    params.update(kw)
    params['buffered'] = True

    # 连接数据库
    engine = _Engine(lambda : mysql.connector.connect(**params))
    logging.info('Init mysql engine <%s> ok.' % hex(id(engine)))



# 数据库引擎对象
class _Engine(object):
    def __init__(self, connect):
        self.connect = connect

    def connect(self):
        return self.connect

def connection():
    """
    db模块核心函数，用于获取一个数据库连接
    通过_ConnectionCtx对 _db_ctx封装，使得惰性连接可以自动获取和释放，
    也就是可以使用 with语法来处理数据库连接
    _ConnectionCtx    实现with语法
    ^
    |
    _db_ctx           _DbCtx实例
    ^
    |
    _DbCtx            获取和释放惰性连接
    ^
    |
    _LasyConnection   实现惰性连接
    """
    return _ConnectionCtx()

def with_connection(func):
    """
    设计一个装饰器 替换with语法，让代码更优雅
    比如:
        @with_connection
        def foo(*args, **kw):
            f1()
            f2()
            f3()
    """
    @functools.wraps(func)
    def _wrapper(*args, **kw):
        with _ConnectionCtx():
            return func(*args, **kw)
    return _wrapper



# 持有数据库联机的上下文对象
class _LasyConnection():
    """
    惰性连接对象
    仅当需要cursor对象时，才连接数据库，获取连接
    """
    def __init__(self):
        self.connection = None

    def cursor(self):
        if self.connection is None:
            self.connection = engine.connect()
            logging.info('[CONNECTION] [OPEN] connection <%s>...' % hex(id(self.connection)))

        return self.connection.cursor()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def cleanup(self):
        if self.connection is not None:
            logging.info('[CONNECTION] [CLOSE] connection <%s>...' % hex(id(self.connection)))
            self.connection.close()
            self.connection = None


class _DbCtx(threading.local):
    def __init__(self):
        self.connection = None
        self.transactions = 0

    def is_init(self):
        return not self.connection is None

    def init(self):
        self.connection = _LasyConnection()
        self.transactions = 0

    def cleanup(self):
        self.connection.cleanup()
        self.connection = None

    def cursor(self):
        return self.connection.cursor()

_dbCtx = _DbCtx()

class _ConnectionCtx(object):
    def __enter__(self):
        global _dbCtx
        self.should_cleanup = False
        if not _dbCtx.is_init():
            _dbCtx.init()
            self.should_cleanup = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        global _dbCtx
        if self.should_cleanup:
            _dbCtx.cleanup()

# select
@with_connection
def _select(sql, first, *args):
    global _dbCtx
    cursor = None
    sql = sql.replace('?', '%s')
    logging.info('sql:%s, args:%s' % (sql, args))

    try:
        cursor = _dbCtx.connection.cursor()
        cursor.execute(sql, args)
        if cursor.description:
            names = [x[0] for x in cursor.description]

        if first:
            values = cursor.fetchone()
            if not values:
                return None
            return Dict(names, values)
        return [Dict(names, x) for x in cursor.fetchall()]
    finally:
        if cursor:
            cursor.close()

def select_one(sql, *args):
    return _select(sql, True, *args)

def select_init(sql, *args):
    d = _select(sql, True, *args)
    if len(d) != 1:
        raise DBError('error')
    return d.values()[0]

def select(sql, *args):
    return _select(sql, False, *args)

@with_connection
def _update(sql, *args):
    cursor = None
    sql = sql.replace('?', '%s')
    logging.info('sql:%s, args:%s' % (sql, args))

    try:
        cursor = _dbCtx.connection.cursor()
        cursor.execute(sql, *args)
        r = cursor.rowcount
        if _dbCtx.transactions == 0:
            logging.info('auto commit')
            _dbCtx.connection.commit()
        return r
    finally:
        if cursor:
            cursor.close()

def update(sql, *args):
    return _update(sql, *args)

def insert(table, **kw):
    cols, args = zip(kw.iteritems())
    sql = 'insert into %s (%s) values (%s)' % (table, ','.join(['%s' % col for col in cols]), ','.join(['?' for i in range(len(cols))]))
    return _update(sql, *args)

class Dict(dict):
    """
    字典对象
    实现一个简单的可以通过属性访问的字典，比如 x.key = value
    """
    def __init__(self, names=(), values=(), **kw):
        super(Dict, self).__init__(**kw)
        for k, v in zip(names, values):
            self[k] = v

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

class _TransactionCtx(object):
    """
    事务嵌套比Connection嵌套复杂一点，因为事务嵌套需要计数，
    每遇到一层嵌套就+1，离开一层嵌套就-1，最后到0时提交事务
    """
    def __enter__(self):
        self.should_close_conn = False
        if not _dbCtx.is_init():
            _dbCtx.init()
            self.should_close_conn = True
        _dbCtx.transactions += 1
        logging.info('begin transaction...' if _db_ctx.transactions == 1 else 'join current transaction...')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        _dbCtx.transactions -= 1
        try:
            if _dbCtx.transactions == 0:
                if exc_type is None:
                    self.commit()
                else:
                    self.rollback()
        finally:
            if self.should_close_conn:
                _dbCtx.cleanup()

    def commit(self):
        logging.info('commit transaction...')
        try:
            _dbCtx.connection.commit()
            logging.info('commit ok')
        finally:
            logging.warning('commit failed. try rollback...')
            _dbCtx.connection.rollback()
            logging.warning('rollback ok.')
            raise

    def rollback(self):
        logging.info('rollback transaction...')
        _dbCtx.connection.rollback()
        logging.info('rollback ok')

def transaction():
    return _TransactionCtx()

def with_transaction(func):
    @functools.wraps(func)
    def wrapper(*args, **kw):
        with transaction():
            return func(*args, **kw)
    return wrapper

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    create_engine('root', '123456', 'otstools', port=3307)
    print _select('select * from menu', True)
