# -*- coding:utf-8 -*-
import time, threading
from task.manage import TaskManager
from task.base import BaseTask

class Task1(BaseTask):
    def __init__(self):
        pass
    def execute(self, requirements):
        print('Task1 with {}'.format(str(requirements)))
        return {'task': 'Task1', 'requirements': requirements}

class Task2(BaseTask):
    def __init__(self):
        pass
    def execute(self, requirements):
        print('Task2 with {}'.format(str(requirements)))
        return {'task': 'Task2', 'requirements': requirements}

class Task3(BaseTask):
    def __init__(self):
        pass
    def execute(self, requirements):
        print('Task3 with {}'.format(str(requirements)))
        return {'task': 'Task3', 'requirements': requirements}

class Task4(BaseTask):
    def __init__(self):
        pass
    def execute(self, requirements):
        print('Task4 with {}'.format(str(requirements)))
        return {'task': 'Task4', 'requirements': requirements}

def translate1(data):
    data.update({'translator': 'translate1'})
    return data

def translate2(data):
    data.update({'translator': 'translate2'})
    return data

def translate3(data):
    data.update({'translator': 'translate3'})
    return data

def test():
    task1 = Task1()
    task2 = Task2()
    task3 = Task3()
    task4 = Task4()
    manager = TaskManager()
    manager.add('task1', task1)
    manager.add('task2-1', task2)
    manager.add('task2-2', task2)
    manager.add('task3', task3)
    manager.add('task4', task4)
    manager.connect('task1', 'task2-1', translate1)
    manager.connect('task1', 'task2-2', translate1)
    manager.connect('task2-1', 'task3', translate2)
    manager.connect('task2-2', 'task4', translate3)
    manager.schedule('task1', {})
    manager.run()
    print('done')

def action(requirements):
    for _ in range(0, 3):
        print('{}: {}'.format(threading.currentThread().ident, str(requirements)))
        time.sleep(requirements['second'])

def test1():
    task = BaseTask(action)
    manager = TaskManager()
    manager.add('action', task)
    manager.schedule('action', {'name':'action 1', 'second': 1})
    manager.schedule('action', {'name':'action 2', 'second': 1})
    manager.schedule('action', {'name':'action 3', 'second': 1})
    manager.run()

def action3(requirements):
    print('Action 3 with {}'.format(str(requirements)))

def action2(requirements):
    print('Action 2 with {}'.format(str(requirements)))

def action1(requirements):
    print(requirements)
    return {'done_by': 'action1', 'requirements': requirements}

def test2():
    task1 = BaseTask(action1)
    task2 = BaseTask(action2)
    task3 = BaseTask(action3)
    manager = TaskManager()
    manager.add('task1', task1)
    manager.add('task2', task2)
    manager.add('task3', task3)
    manager.connect('task1', 'task2')
    manager.connect('task1', 'task3')
    manager.schedule('task1', {})
    manager.run()

class CounterTask(BaseTask):
    def __init__(self):
        self.count = 0
    def execute(self, requirements):
        time.sleep(1)
        self.count += 1
        return {'count': str(self.count)}

class CallerTask(BaseTask):
    def __init__(self):
        pass
    def execute(self, requirements):
        print(requirements)
        return requirements

def error_handle(task_name, task_instance, error):
    print('{}: {}'.format(task_name, str(error)))

def test3():
    caller = CallerTask()
    counter = CounterTask()
    manager = TaskManager(error_handle=error_handle)
    manager.add('caller', caller)
    manager.add('counter', counter)
    manager.connect('caller', 'counter')
    manager.connect('counter', 'caller')
    manager.schedule('caller', {})
    manager.run()

def main():
    test()
    print('')

if __name__ == '__main__':
    main()
