from __future__ import print_function

import os

from gcloud.rest.taskqueue import encode
from gcloud.rest.taskqueue import TaskQueue


def test_lifecycle():
    project = os.environ['GCLOUD_PROJECT']
    task_queue = 'test-pull'

    payload = 'do-the-lifecycle'

    tq = TaskQueue(project, task_queue)

    # drain old test tasks
    tq.drain()

    inserted = tq.insert(encode(payload), tag=encode('gcloud-rest-queue-test'))
    print(inserted)

    got = tq.get(inserted['name'], full=True)
    print(got)

    assert inserted == got

    listed = tq.list(full=True)
    print(listed)

    assert listed and listed['tasks']
    assert inserted in listed['tasks']

    leased = {'name': {'whyIsThisADict': 'subscriptableLinting'}}
    while leased['name'] != inserted['name']:
        leased_list = tq.lease(num_tasks=1)
        print(leased_list)

        assert len(leased_list['tasks']) == 1

        leased = leased_list['tasks'][0]
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
