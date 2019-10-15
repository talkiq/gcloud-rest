import os

import setuptools


PACKAGE_ROOT = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(PACKAGE_ROOT, 'README.rst')) as f:
    README = f.read()


setuptools.setup(
    name='gcloud-rest',
    version='3.0.0',
    description='[deprecated] RESTful Python Client for Google Cloud',
    long_description=README,
    namespace_packages=[
        'gcloud',
        'gcloud.rest',
    ],
    packages=setuptools.find_packages(exclude=('tests',)),
    install_requires=[],
    author='Vi Engineering',
    author_email='voiceai-eng@dialpad.com',
    url='https://github.com/talkiq/gcloud-aio',
    platforms='Posix; MacOS X; Windows',
    include_package_data=True,
    zip_safe=False,
    license='MIT License',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
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
