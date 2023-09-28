#!/usr/bin/env python
# encoding: utf-8
#
# This file is part of ckanext-versioned-datastore
# Created by the Natural History Museum in London, UK
from ckan.plugins import toolkit

from . import datastore, search, downloads, status

blueprints = [datastore.blueprint, search.blueprint, status.blueprint]

is_debug = toolkit.asbool(toolkit.config.get('debug', toolkit.config.get('DEBUG')))
if is_debug:
    blueprints.append(downloads.blueprint)
