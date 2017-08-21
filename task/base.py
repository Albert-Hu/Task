# -*- coding:utf-8 -*-

def _do_nothing(requirements):
    return requirements

class BaseTask(object):
    def __init__(self, execute=_do_nothing):
        assert callable(execute), 'Argument should be a function.'
        self.execute = execute
