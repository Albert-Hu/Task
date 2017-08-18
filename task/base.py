# -*- coding:utf-8 -*-

def _doNothing(requirements):
    pass

class BaseTask(object):
    def __init__(self, execute=_doNothing):
        assert callable(execute), 'Argument should be a function.'
        self.execute = execute
