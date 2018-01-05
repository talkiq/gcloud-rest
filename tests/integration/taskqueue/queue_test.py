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
    got = tq.get(inserted['name'], full=True)
    assert inserted == got

    listed = tq.list(full=True)
    assert len(listed['tasks']) == 1
    assert inserted == listed['tasks'][0]

    leased = tq.lease()
    assert leased['pullMessage']['payload'] == payload
    assert inserted == leased

    renewed = tq.renew(leased)
    assert renewed == leased

    # ack?
    # cancel?

    tq.delete(renewed['name'])
