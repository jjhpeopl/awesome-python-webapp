# _*_ coding: utf-8 _*_
__author__ = 'jjh'

import threading

# 数据库引擎对象
class _Engine(object):
    def __init__(self, connect):
        self.connect = connect

    def connect(self):
        return self.connect

engin = None

# 持有数据库联机的上下文对象
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

def connection():
    return _ConnectionCtx()