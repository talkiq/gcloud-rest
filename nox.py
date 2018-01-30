# pylint: disable=import-self,no-member
import os

import nox


@nox.session
@nox.parametrize('python_version', ['2.7', '3.4', '3.5', '3.6'])
def unit_tests(session, python_version):
    session.interpreter = 'python{}'.format(python_version)
    session.virtualenv_dirname = 'unit-' + python_version

    session.install('pytest', 'pytest-cov')
    session.install('-e', '.')

    session.run(
        'py.test',
        '--quiet',
        '--cov=gcloud.rest',
        '--cov=tests.unit',
        '--cov-append',
        '--cov-report=',
        '--cov-fail-under=32',
        os.path.join('tests', 'unit'),
        *session.posargs)


@nox.session
@nox.parametrize('python_version', ['2.7', '3.6'])
def integration_tests(session, python_version):
    if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
        session.skip('Credentials must be set via environment variable.')

    session.interpreter = 'python{}'.format(python_version)
    session.virtualenv_dirname = 'integration-' + python_version

    session.install('pytest', 'pytest-cov', 'pytest-mock')
    session.install('.')

    session.run(
        'py.test',
        '--quiet',
        '--cov=gcloud.rest',
        '--cov=tests.integration',
        '--cov-append',
        '--cov-report=',
        '--cov-fail-under=55',
        os.path.join('tests', 'integration'),
        *session.posargs)


@nox.session
@nox.parametrize('python_version', ['2.7', '3.6'])
def lint_setup_py(session, python_version):
    session.interpreter = 'python{}'.format(python_version)
    session.virtualenv_dirname = 'setup'

    session.install('docutils', 'Pygments')
    session.run(
        'python',
        'setup.py',
        'check',
        '--restructuredtext',
        '--strict')


@nox.session
@nox.parametrize('python_version', ['3.6'])
def cover(session, python_version):
    session.interpreter = 'python{}'.format(python_version)
    session.virtualenv_dirname = 'cover'

    session.install('codecov', 'coverage', 'pytest-cov')

    session.run('coverage', 'report', '--show-missing', '--fail-under=36')
    session.run('codecov')
    session.run('coverage', 'erase')
