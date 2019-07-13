RESTful Google Cloud Client Library for Python
==============================================

This project is a collection of Google Cloud client libraries for the REST-only
APIs; its *raison d'etre* is to implement a simple `CloudTasks API`_ as well as
a more abstract TaskManager. That... isn't as relevant now that Google has
deprecated PullQueue-style tasks, but this library will continue to be OSS to
provide support to anyone looking for a concurrency-friendly approach to
interacting with the GCP APIs in Python 2 and 3.

If you don't need to support Python 2, you probably want to use `gcloud-aio`_,
which has support for all the same APIs (and more!) and additionally includes
native support for ``asyncio``.

|pypi| |circleci| |coverage| |pythons|

Some of the client libraries live in separate packages:

- |pypids| `Google Cloud Datastore`_ (`Datastore README`_)

Note that all the modules will eventually be released separately, and the
``gcloud-rest`` package will be deprecated.

Installation
------------

.. code-block:: console

    $ pip install --upgrade gcloud-rest

(or, for the packages released independently):

.. code-block:: console

    $ pip install --upgrade gcloud-rest-datastore

Usage
-----

This project currently exposes interfaces to ``CloudTasks``, ``Datastore``,
``KMS``, and ``Storage``.

For the libraries that are packaged separately, you can find usage examples
in the corresponding README file.

For the others, keep reading.

Storage (see `bucket.py`_):

.. code-block:: python

    from gcloud.rest.storage import Bucket

    bk = Bucket('my-project', 'bucket-name')

    # list all objects
    objects_in_bucket = bk.list_objects()
    # list objects with prefix
    objects_in_bucket = bk.list_objects(prefix='in/subdir/')

    object = bk.download('object-name')
    object_contents = bk.download_as_string('object-name')

KMS (see `client.py`_):

.. code-block:: python

    from gcloud.rest.kms import KMS
    from gcloud.rest.core import encode

    kms = KMS('my-project', 'my-keyring', 'my-key-name')

    # encrypt
    plaintext = 'the-best-animal-is-the-aardvark'
    ciphertext = kms.encrypt(encode(plaintext))

    # decrypt
    assert kms.decode(encode(ciphertext)) == plaintext

TaskQueue (for ``CloudTasks``, see `queue.py`_):

.. code-block:: python

    from gcloud.rest.core import decode
    from gcloud.rest.core import encode
    from gcloud.rest.taskqueue import TaskQueue

    tq = TaskQueue('my-project', 'taskqueue-name')

    # create a task
    payload = 'aardvarks-are-awesome'
    tq.insert(encode(payload))

    # list and get tasks
    tasks = tq.list()
    random_task = tasks.get('tasks')[42]
    random_task_body = tq.get(random_task['name'])

    # lease, renew, and ack/cancel/delete tasks
    task_leases = tq.lease(num_tasks=3)
    tasks = task_lease.get('tasks')
    # assert len(tasks) <= 3

    for task in tasks:
        payload = decode(task['pullMessage']['payload']).decode()

        # you'll need to renew the task if you take longer than
        # task['scheduleTime'] to process it
        tq.renew(task)

        # do something with payload

        if success:
            tq.ack(task)
        elif temporary_failure:
            tq.cancel(task)
        elif permanent_failure:
            tq.delete(task['name'])

TaskManager (for ``CloudTasks``, see `manager.py`_):

.. code-block:: python

    from gcloud.rest.taskqueue import FailFastError
    from gcloud.rest.taskqueue import TaskManager

    def worker_method(payloads):
        for task in payloads:
            # do something with the task

            if success:
                yield 'anything'
            elif temporary_failure:
                yield Exception('insert message here')
            elif permanent_failure:
                yeild FailFastError('insert message here')

    tm = TaskManager('my-project', 'taskqueue-name', worker_method)
    tm.find_tasks_forever()

.. _bucket.py: https://github.com/talkiq/gcloud-rest/blob/master/gcloud/rest/storage/bucket.py
.. _client.py: https://github.com/talkiq/gcloud-rest/blob/master/gcloud/rest/kms/client.py
.. _CloudTasks API: https://cloud.google.com/cloud-tasks/docs/reference/rest/v2beta2/projects.locations.queues.tasks
.. _Datastore README: https://github.com/talkiq/gcloud-rest/blob/master/datastore/README.rst
.. _gcloud-aio: https://github.com/talkiq/gcloud-aio
.. _Google Cloud Datastore: https://pypi.org/project/gcloud-rest-datastore/
.. _manager.py: https://github.com/talkiq/gcloud-rest/blob/master/gcloud/rest/taskqueue/manager.py
.. _queue.py: https://github.com/talkiq/gcloud-rest/blob/master/gcloud/rest/taskqueue/queue.py

.. |pypi| image:: https://img.shields.io/pypi/v/gcloud-rest.svg?style=flat-square
    :alt: Latest PyPI Version
    :target: https://pypi.org/project/gcloud-rest/

.. |circleci| image:: https://img.shields.io/circleci/project/github/talkiq/gcloud-rest/master.svg?style=flat-square
    :alt: CircleCI Test Status
    :target: https://circleci.com/gh/talkiq/gcloud-rest/tree/master

.. |coverage| image:: https://img.shields.io/codecov/c/github/talkiq/gcloud-rest/master.svg?style=flat-square
    :alt: Code Coverage
    :target: https://codecov.io/gh/talkiq/gcloud-rest

.. |pythons| image:: https://img.shields.io/pypi/pyversions/gcloud-rest.svg?style=flat-square
    :alt: Python Version Support
    :target: https://pypi.org/project/gcloud-rest/

.. |pypids| image:: https://img.shields.io/pypi/v/gcloud-rest-datastore.svg?style=flat-square
    :alt: Latest PyPI Version
    :target: https://pypi.org/project/gcloud-rest-datastore/
