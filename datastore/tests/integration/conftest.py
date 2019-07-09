import os

import pytest


@pytest.fixture(scope='module')  # type: ignore
def creds():
    # type: () -> str
    # TODO: bundle public creds into this repo
    return os.environ['GOOGLE_APPLICATION_CREDENTIALS']


@pytest.fixture(scope='module')  # type: ignore
def kind():
    # type: () -> str
    return 'public_test'


@pytest.fixture(scope='module')  # type: ignore
def project():
    # type: () -> str
    return 'voiceai-staging'


@pytest.fixture(scope='module')  # type: ignore
def export_bucket_name():
    # type: () -> str
    return 'voiceai-staging-public-test'
