import json
import os
import time

import pytest

from gcloud.rest.taskqueue import encode
from gcloud.rest.taskqueue import TaskManager


def test_lifecycle(caplog, mocker):
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

    tm = TaskManager(project, task_queue, worker, batch_size=len(tasks),
                     lease_seconds=10)

    # drain old test tasks
    drain = tm.tq.drain()

    # insert new ones
    for task in tasks:
        tm.tq.insert(encode(json.dumps(task)),
                     tag=encode('gcloud-rest-manager-test-lifecycle'))

    tm.find_and_process_work()
    assert worker.mock_calls == [mocker.call(tasks)]
    for record in caplog.records:
        assert record.levelname != 'ERROR'


@pytest.mark.slow
def test_multiple_leases(caplog, mocker):
    project = os.environ['GCLOUD_PROJECT']
    task_queue = 'test-pull'

    tasks = [
        {'test_idx': 1},
        {'test_idx': 2},
    ]

    def succeed_after_multiple_leases(ts):
        time.sleep(10)
        return ['ok' for _ in ts]

    worker = mocker.Mock()
    worker.side_effect = succeed_after_multiple_leases

    tm = TaskManager(project, task_queue, worker, batch_size=len(tasks),
                     lease_seconds=4)

    # drain old test tasks
    drain = tm.tq.drain()

    # insert new ones
    for task in tasks:
        tm.tq.insert(encode(json.dumps(task)),
                     tag=encode('gcloud-rest-manager-test-multilease'))

    caplog.clear()
    tm.find_and_process_work()
    assert worker.mock_calls == [mocker.call(tasks)]
    for record in caplog.records:
        assert record.levelname != 'ERROR'


@pytest.mark.slow
def test_multiple_leases_churn(caplog, mocker):
    project = os.environ['GCLOUD_PROJECT']
    task_queue = 'test-pull'

    tasks = [
        {'test_idx': 1},
        {'test_idx': 2},
    ]

    def succeed_after_multiple_leases(ts):
        _ = [x**2 for x in range(40000000)]
        return ['ok' for _ in ts]

    worker = mocker.Mock()
    worker.side_effect = succeed_after_multiple_leases

    tm = TaskManager(project, task_queue, worker, batch_size=len(tasks),
                     lease_seconds=4)

    # drain old test tasks
    drain = tm.tq.drain()
    # insert new ones
    for task in tasks:
        tm.tq.insert(encode(json.dumps(task)),
                     tag=encode('gcloud-rest-manager-test-multilease'))

    caplog.clear()
    tm.find_and_process_work()
    assert worker.mock_calls == [mocker.call(tasks)]
    for record in caplog.records:
        assert record.levelname != 'ERROR'
