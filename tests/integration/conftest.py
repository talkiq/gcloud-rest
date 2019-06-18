import os

import pytest


@pytest.fixture(scope='module')
def creds():
    return os.environ['GOOGLE_APPLICATION_CREDENTIALS']


@pytest.fixture(scope='module')
def project():
    return 'voiceai-staging'
