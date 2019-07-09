Contributing to ``gcloud-rest``
===============================

Thanks for contributing to ``gcloud-rest``! We appreciate contributions of any
size and hope to make it easy for you to dive in. Here's the thousand-foot
overview of how we've set this project up.

Project Setup
-------------

Our vision is that each module (``auth``, ``core``, ``kms``, etc.) should be
released in its own PyPI package.

Some modules are still bundled together in the ``gcloud-rest`` package. If you
are making changes to one of those modules, please consider creating another
PR to create a separate package for it. You can get inspiration from the
`datastore module`_.

Testing
-------

Tests are run with `nox`_. See each project's ``noxfile.py`` for the scaffolding
and the ``tests/unit`` and ``tests/integration`` folders for the actual test
code.

You can get nox with ``pip install nox`` and run a specific project's tests with
``nox -f <project-folder>/noxfile.py``.

Modules in the ``gcloud-rest`` package are tested together, using the root
``noxfile.py``. You can run their tests by running ``nox``.

Local Development
~~~~~~~~~~~~~~~~~

We recommend using ``nox``, as described above, but this library supports using
more standard workflows as well. For more convenient local development, or if
you don't want to use ``nox``, you can run the tests using pytest:

- create and activate a virtual environment: ``python -m venv venv && source venv/bin/activate``
- install test dependencies: ``pip install pytest``
- install library from local path: ``pip install -e .``

Then, you can run any tests manually with:

.. code-block:: console

    python -m pytest tests/unit/<your favourite test>

Submitting Changes
------------------

Please send us a `Pull Request`_ with a clear list of what you've done. When
you submit a PR, we'd appreciate test coverage of your changes (and feel free
to test other things; we could always use more and better tests!).

Please make sure all tests pass and your commits are atomic (one feature per
commit).

Always write a clear message for your changes. We think the
`conventional changelog`_ message format is pretty cool and try to stick to it
as best we can (we even generate release notes from it automatically!).

Roughly speaking, we'd appreciate if your commits looked like this:

.. code-block:: console

    feat(taskqueue): implemented task queue manager

    Created gcloud.rest.taskqueue.TaskManager for an abstraction layer around
    renewing leases on pull-queue tasks. Handles auto-renewal.

The first line is the most specific in this format; it should have the format
``type(project): message``, where:

- ``type`` is one of ``feat``, ``fix``, ``docs``, ``refactor``, ``style``, ``perf``, ``test``, or ``chore``
- ``project`` is ``auth``, ``bigquery``, ``datastore``, etc.
- ``message`` is a concise description of the patch and brings the line to no more than 72 characters

Coding Conventions
------------------

We use `pre-commit`_ to manage our coding conventions and linting. You can
install it with ``pip install pre-commit`` and set it to run pre-commit hooks
for ``gcloud-rest`` by running ``pre-commit install``. The same linters get run
in CI against all changesets.

You can also run ``pre-commit`` in an ad-hoc fashion by calling
``pre-commit run --all-files``.

CircleCI will also make sure that the code is correct in Python 2.7.
To check that locally, run ``pre-commit run -c .pre-commit-config.py27.yaml --all-files``
in a Python 2.7 environment.

It may be useful to have two Python virtual environments and run pre-commit in both.

Python 2.7:

.. code-block:: console

    $ virtualenv venv-27
    $ source venv-27/bin/activate
    $ pip install pre-commit
    $ pre-commit run -c .pre-commit-config.py27.yaml --all-files

Python 3.7:

.. code-block:: console

    $ python3 -m venv venv-37
    $ source venv-37/bin/activate
    $ pip install pre-commit
    $ pre-commit run --all-files


Other than the above enforced standards, we like code that is easy-to-read for
any new or returning contributors with relevant comments where appropriate.

Releases
--------

If you are a maintainer looking to release a new version, see our
`Release documentation`_.

.. _conventional changelog: https://github.com/conventional-changelog/conventional-changelog
.. _datastore module: https://github.com/talkiq/gcloud-rest/blob/master/datastore
.. _nox: https://nox.readthedocs.io/en/latest/
.. _pre-commit: http://pre-commit.com/
.. _Pull Request: https://github.com/talkiq/gcloud-rest/pull/new/master
.. _Release documentation: https://github.com/talkiq/gcloud-rest/blob/master/.github/RELEASE.rst

Thanks for your contribution!

With love,
TalkIQ
