#!/usr/bin/env python
# -*- coding: utf-8 -*-


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read().replace('.. :changelog:', '')

requirements = [
    'MySQL-python',
    'python-swiftclient'
]

test_requirements = [
    # TODO: put package test requirements here
]

setup(
    name='swiftbulkuploader',
    version='0.1.0',
    description="Swift Bulk Uploader makes it easy to upload entire directories to the OLRC. The Ontario Library Research Cloud (OLRC) project is a collaboration of Ontario’s university libraries to build a high capacity, geographically distributed storage network, based on OpenStack and the Swift object store.",
    long_description=readme + '\n\n' + history,
    author="OLRC Collaborators",
    author_email='cloudtech@scholarsportal.info',
    url='https://github.com/OLRC/swiftbulkuploader',
    packages=[
        'swiftbulkuploader',
    ],
    package_dir={'swiftbulkuploader':
                 'swiftbulkuploader'},
    include_package_data=True,
    install_requires=requirements,
    license="BSD",
    zip_safe=False,
    keywords='swiftbulkuploader',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python :: 2.7',
    ],
    test_suite='tests',
    tests_require=test_requirements,
    entry_points={
        'console_scripts': [
            'prepareupload=swiftbulkuploader.prepareupload:main',
            'bulkupload=swiftbulkuploader.bulkupload:main'
        ]
    }
)
