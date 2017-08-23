# -*- coding:utf-8 -*-

def _bypass(*args):
    return args

class BaseTask(object):
    def __init__(self, execute=None):
        if execute != None:
            assert callable(execute), 'Argument should be a function.'
            self.execute = execute
    def execute(self, *args):
        return args
