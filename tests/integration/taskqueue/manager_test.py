import json
import os
import unittest.mock

import gcloud.rest.taskqueue.manager as manager


def test_lifecycle():
    project = os.environ['GCLOUD_PROJECT']
    task_queue = 'test-pull'

    tasks = [
        {'test_idx': 1},
        {'test_idx': 2},
        {'test_idx': 3},
        {'test_idx': 4},
    ]

    worker = unittest.mock.Mock()
    worker.return_value = ['ok' for _ in tasks]

    tm = manager.TaskManager(project, task_queue, worker,
                             batch_size=len(tasks))

    # drain old test tasks
    drain = tm.tq.drain()

    # insert new ones
    for task in tasks:
        tm.tq.insert(json.dumps(task).encode())

    tm.find_and_process_work()
    assert worker.mock_calls == [unittest.mock.call(tasks)]
