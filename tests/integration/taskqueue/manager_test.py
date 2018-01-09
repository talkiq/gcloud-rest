import json
import os

from gcloud.rest.taskqueue import encode
from gcloud.rest.taskqueue import TaskManager


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

    tm = TaskManager(project, task_queue, worker, batch_size=len(tasks))

    # drain old test tasks
    drain = tm.tq.drain()

    # insert new ones
    for task in tasks:
        tm.tq.insert(encode(json.dumps(task)))

    tm.find_and_process_work()
    assert worker.mock_calls == [mocker.call(tasks)]
