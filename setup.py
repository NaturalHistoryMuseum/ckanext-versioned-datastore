#!/usr/bin/env python
# encoding: utf-8
#
# This file is part of ckanext-versioned-datastore
# Created by the Natural History Museum in London, UK

from setuptools import find_packages, setup

__version__ = u'1.0.0-alpha'

with open(u'README.md', u'r') as f:
    __long_description__ = f.read()

dependencies = {'eevee': 'git+https://github.com/NaturalHistoryMuseum/eevee@v1.2.3#egg=eevee-1.2.3'}

setup(
    name=u'ckanext-versioned-datastore',
    version=__version__,
    description=u'A CKAN extension providing a versioned datastore using MongoDB and Elasticsearch.',
    long_description=__long_description__,
    classifiers=[
        u'Development Status :: 3 - Alpha',
        u'Framework :: Flask',
        u'Programming Language :: Python :: 2.7'
    ],
    keywords=u'CKAN data elastic versioning',
    author=u'Natural History Museum',
    author_email=u'data@nhm.ac.uk',
    url=u'https://github.com/NaturalHistoryMuseum/ckanext-versioned-datastore',
    license=u'GNU GPLv3',
    packages=find_packages(exclude=[u'tests']),
    namespace_packages=[u'ckanext', u'ckanext.versioned_datastore'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        u'backports.csv==1.0.6',
        u'cchardet==2.1.4',
        u'contextlib2>=0.6.0.post1',
        u'openpyxl==2.5.8',
        u'requests',
        u'six>=1.11.0',
        u'xlrd==1.1.0',
        u'elasticsearch>=6.0.0,<7.0.0',
        u'elasticsearch-dsl>=6.0.0,<7.0.0',
        u'pyrsistent~=0.16.1',
        u'jsonschema>=3.0.0',
    ] + [u'{0} @ {1}'.format(k, v) for k, v in dependencies.items()],
    dependency_links=dependencies.values(),
    entry_points= \
        u'''
        [ckan.plugins]
            versioned_datastore=ckanext.versioned_datastore.plugin:VersionedSearchPlugin
        ''',
    )
