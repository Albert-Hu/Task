from task.base import BaseTask
from task.manage import TaskManager

excepted_value = 0
task_number = 1000

class TaskNode(BaseTask):
    def __init__(self):
        pass
    def execute(self, value):
        return value + 1

class TaskEndNode(BaseTask):
    def __init__(self):
        pass
    def execute(self, value):
        global excepted_value
        excepted_value = value

def error_handle(task_name, task_instance, error):
    print('[ERROR] {}: {}'.format(task_name, str(error)))

def run():
    global task_number
    start = TaskNode()
    end = TaskEndNode()
    manager = TaskManager(error_handle)
    manager.add('start', start)
    manager.add('end', end)
    previous = 'start'
    for n in range(0, task_number):
        task = TaskNode()
        current = 'Task{}'.format(n)
        manager.add(current, task)
        manager.connect(previous, current)
        previous = current
    manager.connect(previous, 'end')
    manager.schedule('start', 0)
    manager.run()
    return (task_number + 1) == excepted_value

if __name__ == '__main__':
    succeed = run()
    print('Test {}.'.format('succeed' if succeed else 'failed'))
