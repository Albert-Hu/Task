# -*- coding:utf-8 -*-
from concurrent.futures import ThreadPoolExecutor
from task.base import BaseTask

def _run(instance, parameters):
    status = {'succeed': False}
    try:
        result = instance.execute(parameters)
        status['data'] = result
        status['succeed'] = True
    except Exception as e:
        status['error'] = e
    return status

def _error_handle(callback, task_name, task_instance, error):
    try:
        callback(task_name, task_instance, error)
    except Exception as e:
        print(e)

def _translate(function, data):
    result = {'succeed': False}
    try:
        new_data = function(data)
        if new_data != None:
            result['succeed'] = True
            result['data'] = new_data
    except Exception as e:
        result['error'] = e
    return result

def _bypass(data):
    return data

def _bypass_error(task_name, task_instance, error):
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
        assert not self._isbusy, 'Task manager is busy.'
        assert isinstance(task, BaseTask), 'Task is not inherit from BaseTask'
        self._tasks[name] = {'instance': task, 'next': {}, 'map': {}}

    def connect(self, _from, _to, translator=_bypass):
        assert not self._isbusy, 'Task manager is busy.'
        assert _from in self._tasks, 'Task {} not found.'.format(_from)
        assert _to in self._tasks, 'Task {} not found.'.format(_to)
        assert callable(translator), 'Argument 3 should be a function.'
        task = self._tasks[_from]
        task['next'][_to] = translator

    def map(self, _from, _to, translator=_bypass):
        assert not self._isbusy, 'Task manager is busy.'
        assert _from in self._tasks, 'Task {} not found.'.format(_from)
        assert _to in self._tasks, 'Task {} not found.'.format(_to)
        assert callable(translator), 'Argument 3 should be a function.'
        task = self._tasks[_from]
        task['map'][_to] = translator

    def schedule(self, name, requirements):
        assert not self._isbusy, 'Task manager is busy.'
        assert name in self._tasks, 'Task {} not found.'.format(name)
        self._todo.insert(0, (name, requirements))

    def run(self):
        assert not self._isbusy, 'Task manager is busy.'
        with ThreadPoolExecutor(20) as threads:
            while len(self._todo) > 0:
                name, requirements = self._todo.pop()
                task = self._tasks[name]
                worker = threads.submit(_run, task['instance'], requirements)
                self._workers.append((name, worker))
            while len(self._workers) > 0:
                name, worker = self._workers.pop()
                if not worker.done():
                    self._workers.insert(0, (name, worker))
                    continue
                task = self._tasks[name]
                status = worker.result()
                if not status['succeed']:
                    _error_handle(self._error_handle, name, task['instance'], status['error'])
                    continue
                # Go to next tasks.
                for next_task_name in task['next']:
                    function = task['next'][next_task_name]
                    translated_status = _translate(function, status['data'])
                    if 'error' in translated_status:
                        _error_handle(self._error_handle, name, task['instance'], translated_status['error'])
                        break
                    if not translated_status['succeed']:
                        continue
                    next_task = self._tasks[next_task_name]
                    worker = threads.submit(_run, next_task['instance'], translated_status['data'])
                    self._workers.insert(0, (next_task_name, worker))
                # Map to next task.
                for next_task_name in task['map']:
                    function = task['map'][next_task_name]
                    translated_status = _translate(function, status['data'])
                    if 'error' in translated_status:
                        _error_handle(self._error_handle, name, task['instance'], translated_status['error'])
                        break
                    if not type(translated_status['data']) is list:
                        _error_handle(self._error_handle, name, task['instance'], BaseException('Translated data is not a list.'))
                        break
                    if not translated_status['succeed']:
                        continue
                    next_task = self._tasks[next_task_name]
                    for data in translated_status['data']:
                        worker = threads.submit(_run, next_task['instance'], data)
                        self._workers.insert(0, (next_task_name, worker))
