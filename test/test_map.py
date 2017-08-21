from task.base import BaseTask
from task.manage import TaskManager
from _functools import reduce

work_number = 1000

class Work():
    def __init__(self):
        self._is_done = False
    def finish(self):
        self._is_done = True
    def done(self):
        return self._is_done

def error_handle(task_name, task_instance, error):
    print('[ERROR] {}: {}'.format(task_name, str(error)))

def finish(work):
    work.finish()

def run():
    global work_number
    works = [Work() for _ in range(0, work_number)]
    manager = TaskManager(error_handle)
    start = BaseTask()
    end = BaseTask(finish)
    manager.add('start', start)
    manager.add('end', end)
    manager.map('start', 'end')
    manager.schedule('start', works)
    manager.run()
    for work in works:
        if not work.done():
            return False
    return True

if __name__ == '__main__':
    succeed = run()
    print('Test {}.'.format('succeed' if succeed else 'failed'))
