from __future__ import print_function

import base64
import os

import gcloud.rest.taskqueue.queue as queue


def test_lifecycle():
    project = os.environ['GCLOUD_PROJECT']
    task_queue = 'test-pull'

    payload = base64.b64encode(b'do-the-lifecycle')

    tq = queue.TaskQueue(project, task_queue)

    # drain old test tasks
    drain = tq.lease(num_tasks=1000)
    if drain:
        for task in drain['tasks']:
            tq.delete(task['name'])

    inserted = tq.insert(payload)
    print(inserted)

    got = tq.get(inserted['name'], full=True)
    print(got)

    assert inserted == got

    listed = tq.list(full=True)
    print(listed)

    assert len(listed['tasks']) == 1
    assert inserted == listed['tasks'][0]

    leased = tq.lease(num_tasks=1)
    print(leased)

    assert len(leased['tasks']) == 1

    leased = leased['tasks'][0]
    print(leased)

    for k, v in leased.items():
        if k == 'scheduleTime':
            assert inserted[k] != v
        elif k == 'status':
            assert not inserted.get(k)
            assert v['attemptDispatchCount'] == 1
        else:
            assert inserted[k] == v

    renewed = tq.renew(leased)
    print(renewed)
    for k, v in renewed.items():
        if k == 'scheduleTime':
            assert leased[k] != v
        else:
            assert leased[k] == v

    # ack?
    # cancel?

    tq.delete(renewed['name'])
