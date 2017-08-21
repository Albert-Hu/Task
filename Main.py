# -*- coding:utf-8 -*-
from test import test_connect, test_map, test_schedule

tests = {
    'test_map': test_map.run,
    'test_connect': test_connect.run,
    'test_schedule': test_schedule.run
    }

def main():
    for case in tests:
        print('[TEST] {:.<20}{}'.format(case, 'succeed' if tests[case]() else 'failed'))
    test_connect.run()
    print('')

if __name__ == '__main__':
    main()
