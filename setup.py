#!/usr/bin/env python
# encoding: utf-8
#
# This file is part of ckanext-versioned-datastore
# Created by the Natural History Museum in London, UK

from setuptools import find_packages, setup

__version__ = '3.1.3'

with open('README.md', 'r') as f:
    __long_description__ = f.read()

dependencies = {'eevee': 'git+https://github.com/NaturalHistoryMuseum/eevee@v1.2.3#egg=eevee-1.2.3'}

setup(
    name='ckanext-versioned-datastore',
    version=__version__,
    description='A CKAN extension providing a versioned datastore using MongoDB and Elasticsearch.',
    long_description=__long_description__,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    keywords='CKAN data elastic versioning',
    author='Natural History Museum',
    author_email='data@nhm.ac.uk',
    url='https://github.com/NaturalHistoryMuseum/ckanext-versioned-datastore',
    license='GNU GPLv3',
    packages=find_packages(exclude=['tests']),
    namespace_packages=['ckanext', 'ckanext.versioned_datastore'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'backports.csv==1.0.6',
        'cchardet==2.1.4',
        'openpyxl==2.5.8',
        'requests',
        'six>=1.11.0',
        'xlrd==1.1.0',
        'elasticsearch>=6.0.0,<7.0.0',
        'elasticsearch-dsl>=6.0.0,<7.0.0',
        'jsonschema==3.0.0',
        'pandas==1.4.1'
    ] + ['{0} @ {1}'.format(k, v) for k, v in dependencies.items()],
    dependency_links=list(dependencies.values()),
    entry_points= \
        '''
        [ckan.plugins]
            versioned_datastore=ckanext.versioned_datastore.plugin:VersionedSearchPlugin
        ''',
    )
