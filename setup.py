import os

import setuptools


PACKAGE_ROOT = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(PACKAGE_ROOT, 'README.rst')) as f:
    README = f.read()

with open(os.path.join(PACKAGE_ROOT, 'requirements.txt')) as f:
    REQUIREMENTS = [r.strip() for r in f.readlines()]


setuptools.setup(
    name='gcloud-rest',
    version='0.6.0',
    description='RESTful Python Client for Google Cloud',
    long_description=README,
    namespace_packages=[
        'gcloud',
        'gcloud.rest',
    ],
    packages=setuptools.find_packages(exclude=('tests',)),
    install_requires=REQUIREMENTS,
    author='TalkIQ',
    author_email='engineering@talkiq.com',
    url='https://github.com/talkiq/gcloud-rest',
    platforms='Posix; MacOS X; Windows',
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: Internet',
    ],
)
