# -*- coding:utf-8 -*-
import time
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

def main():
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
    manager.translate('task1', 'task2-1', translate1)
    manager.translate('task1', 'task2-2', translate1)
    manager.translate('task2-1', 'task3', translate2)
    manager.translate('task2-2', 'task4', translate3)
    manager.schedule('task1', {})
    manager.run()
    print('done')

if __name__ == '__main__':
    main()
