import base64
import json
import os

import gcloud.rest.taskqueue.manager as manager


def test_lifecycle(mocker):
    project = os.environ['GCLOUD_PROJECT']
    task_queue = 'test-pull'

    tasks = [
        {'test_idx': 1},
        {'test_idx': 2},
        {'test_idx': 3},
        {'test_idx': 4},
    ]

    worker = mocker.Mock()
    worker.return_value = ['ok' for _ in tasks]

    tm = manager.TaskManager(project, task_queue, worker,
                             batch_size=len(tasks))

    # drain old test tasks
    drain = tm.tq.drain()

    # insert new ones
    for task in tasks:
        tm.tq.insert(base64.b64encode(json.dumps(task).encode()).decode())

    tm.find_and_process_work()
    assert worker.mock_calls == [mocker.call(tasks)]
