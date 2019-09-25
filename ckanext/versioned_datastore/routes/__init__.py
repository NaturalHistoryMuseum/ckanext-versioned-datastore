#!/usr/bin/env python
# encoding: utf-8
#
# This file is part of ckanext-versioned-datastore
# Created by the Natural History Museum in London, UK

from . import datastore, search

blueprints = [datastore.blueprint, search.blueprint]
