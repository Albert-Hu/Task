from task.base import BaseTask
from task.manage import TaskManager

test_succeed = True

def error_handle(task_name, task_instance, error):
    global test_succeed
    test_succeed = False
    print('[ERROR] {}: {}'.format(task_name, str(error)))

def run():
    global test_succeed
    manager = TaskManager(error_handle)
    for n in range(0, 1000):
        task = BaseTask()
        name = 'Task{}'.format(n)
        manager.add(name, task)
        manager.schedule(name, None)
    manager.run()
    return test_succeed

if __name__ == '__main__':
    succeed = run()
    print('Test {}.'.format('succeed' if succeed else 'failed'))
