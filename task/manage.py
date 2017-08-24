# -*- coding:utf-8 -*-
from concurrent.futures import ThreadPoolExecutor
from task.base import BaseTask

def _run(instance, args):
    succeed = False
    result = Exception('No task execute.')
    try:
        result = instance.execute(*args)
        succeed = True
    except Exception as e:
        result = e
    return succeed, result

def _error_handle(callback, _from, _to, error):
    try:
        callback(_from, _to, error)
    except Exception as e:
        print(e)

def _translate(function, data):
    succeed = False
    result = Exception('No data translated.')
    try:
        new_data = function(data)
        if isinstance(new_data, tuple) or new_data == None:
            succeed = True
            result = new_data
        else:
            result = Exception('Translated data must be type of tuple.')
    except Exception as e:
        result = e
    return succeed, result

def _multiple_translate(function, data):
    succeed = False
    result = Exception('No data translated.')
    try:
        new_data = function(data)
        if isinstance(new_data, list):
            for d in new_data:
                if not isinstance(d, tuple):
                    result = Exception('Translated data in list must be type of tuple.')
                    break
            else:
                succeed = True
                result = new_data
        elif new_data == None:
            succeed = True
            result = None
        else:
            result = Exception('Translated data must be type of list.')
    except Exception as e:
        result = e
    return succeed, result

def _bypass_multiple(data):
    return data

def _bypass(data):
    return (data,) if data != None else None

def _bypass_error(_from, _to, error):
    pass

class TaskManager(object):
    def __init__(self, error_handle=_bypass_error):
        assert callable(error_handle), 'Argument 3 should be a function.'
        self._error_handle = error_handle
        self._tasks = {}
        self._workers = []
        self._todo = []
        self._isbusy = False

    def add(self, name, task):
        assert not name in self._tasks, 'Task already exists.'
        assert not self._isbusy, 'Task manager is busy.'
        assert isinstance(task, BaseTask), 'Task is not inherit from BaseTask'
        self._tasks[name] = {'instance': task, 'next': {}, 'map': {}}

    def connect(self, _from, _to, translator=_bypass):
        assert not self._isbusy, 'Task manager is busy.'
        assert _from in self._tasks, 'Task {} is not defined.'.format(_from)
        assert _to in self._tasks, 'Task {} is not defined.'.format(_to)
        assert callable(translator), 'Argument 3 should be a function.'
        task = self._tasks[_from]
        task['next'][_to] = translator

    def map(self, _from, _to, translator=_bypass_multiple):
        assert not self._isbusy, 'Task manager is busy.'
        assert _from in self._tasks, 'Task {} is not defined.'.format(_from)
        assert _to in self._tasks, 'Task {} is not defined.'.format(_to)
        assert callable(translator), 'Argument 3 should be a function.'
        task = self._tasks[_from]
        task['map'][_to] = translator

    def schedule(self, name, *args):
        assert not self._isbusy, 'Task manager is busy.'
        assert name in self._tasks, 'Task {} not found.'.format(name)
        self._todo.insert(0, (name, args))

    def run(self):
        assert not self._isbusy, 'Task manager is busy.'
        with ThreadPoolExecutor(20) as threads:
            while len(self._todo) > 0:
                name, args = self._todo.pop()
                task = self._tasks[name]
                worker = threads.submit(_run, task['instance'], args)
                self._workers.append((name, worker))
            while len(self._workers) > 0:
                name, worker = self._workers.pop()
                if not worker.done():
                    self._workers.insert(0, (name, worker))
                    continue
                task = self._tasks[name]
                succeed, task_result = worker.result()
                if not succeed:
                    _error_handle(self._error_handle, name, name, task_result)
                    continue
                # Go to next tasks.
                for next_task_name in task['next']:
                    function = task['next'][next_task_name]
                    succeed, result = _translate(function, task_result)
                    if not succeed:
                        _error_handle(self._error_handle, name, next_task_name, result)
                        continue
                    if result == None:
                        continue
                    next_task = self._tasks[next_task_name]
                    worker = threads.submit(_run, next_task['instance'], result)
                    self._workers.insert(0, (next_task_name, worker))
                # Map to next task.
                for next_task_name in task['map']:
                    function = task['map'][next_task_name]
                    succeed, result = _multiple_translate(function, task_result)
                    if not succeed:
                        _error_handle(self._error_handle, name, next_task_name, result)
                        continue
                    if result == None:
                        continue
                    next_task = self._tasks[next_task_name]
                    for args in result:
                        worker = threads.submit(_run, next_task['instance'], args)
                        self._workers.insert(0, (next_task_name, worker))
        return True
