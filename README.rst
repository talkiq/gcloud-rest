RESTful Google Cloud Client Library for Python
==============================================

This project is a collection of Google Cloud client libraries for the REST-only
APIs; its *raison d'etre* is to implement a simple `CloudTasks API`_ as well as
a more abstract TaskManager.

|pypi| |circleci| |coverage| |pythons|

Installation
------------

.. code-block:: console

    $ pip install --upgrade gcloud-rest

Usage
-----
.. code-block::python
    from gcloud.rest.taskqueue import TaskQueue
    q = TaskQueue('queue', 'taskqueue-name')
    q.list()
    task = tq.lease(num_tasks=3)

Check `queue.py` for all the methods

.. _CloudTasks API: https://cloud.google.com/cloud-tasks/docs/reference/rest/v2beta2/projects.locations.queues.tasks

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
