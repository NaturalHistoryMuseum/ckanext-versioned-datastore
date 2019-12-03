#!/usr/bin/env python
# encoding: utf-8
#
# This file is part of ckanext-versioned-datastore
# Created by the Natural History Museum in London, UK
from ckan.plugins import toolkit

from . import datastore, search, downloads

blueprints = [datastore.blueprint, search.blueprint]

if toolkit.config.get(u'debug', False):
    blueprints.append(downloads.blueprint)
