# -*- coding:utf-8 -*-

def _bypass(*args):
    return args

class BaseTask(object):
    def __init__(self, execute=_bypass):
        assert callable(execute), 'Argument should be a function.'
        self.execute = execute
