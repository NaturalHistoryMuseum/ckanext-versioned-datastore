<img src=".github/nhm-logo.svg" align="left" width="150px" height="100px" hspace="40"/>

# ckanext-versioned-datastore

[![Travis](https://img.shields.io/travis/NaturalHistoryMuseum/ckanext-versioned-datastore/master.svg?style=flat-square)](https://travis-ci.org/NaturalHistoryMuseum/ckanext-versioned-datastore)
[![Coveralls](https://img.shields.io/coveralls/github/NaturalHistoryMuseum/ckanext-versioned-datastore/master.svg?style=flat-square)](https://coveralls.io/github/NaturalHistoryMuseum/ckanext-versioned-datastore)
[![CKAN](https://img.shields.io/badge/ckan-2.9.0a-orange.svg?style=flat-square)](https://github.com/ckan/ckan)

_A CKAN extension providing a versioned datastore using MongoDB and Elasticsearch._

# Overview

This plugin provides a complete replacement for ckan's datastore plugin and therefore shouldn't be used in conjunction with it.
Rather than storing data in PostgreSQL, resource data is stored in MongoDB and then made available to frontend APIs using Elasticsearch.

This allows this plugin to:

  - provide full versioning of resource records - records can be updated when new resource data is uploaded without preventing access to the old data
  - expose advanced search features using Elasticsearch's extensive feature set
  - achieve fast search response times, particularly when compared to PostgreSQL, due Elasticsearch's search performance
  - store large resources (millions of rows) and still provide high speed search responses
  - store complex data as both MongoDB and Elasticsearch are JSON based, allowing object nesting and arrays

This plugin is built on [Eevee](https://github.com/NaturalHistoryMuseum/eevee).


# Installation

Path variables used below:
- `$INSTALL_FOLDER` (i.e. where CKAN is installed), e.g. `/usr/lib/ckan/default`
- `$CONFIG_FILE`, e.g. `/etc/ckan/default/development.ini`

1. Clone the repository into the `src` folder:

  ```bash
  cd $INSTALL_FOLDER/src
  git clone https://github.com/NaturalHistoryMuseum/ckanext-versioned-datastore.git
  ```

2. Activate the virtual env:

  ```bash
  . $INSTALL_FOLDER/bin/activate
  ```

3. Install the requirements from requirements.txt:

  ```bash
  cd $INSTALL_FOLDER/src/ckanext-versioned-datastore
  pip install -r requirements.txt
  ```

4. Run setup.py:

  ```bash
  cd $INSTALL_FOLDER/src/ckanext-versioned-datastore
  python setup.py develop
  ```

5. Add 'versioned_datastore' to the list of plugins in your `$CONFIG_FILE`:

  ```ini
  ckan.plugins = ... versioned_datastore
  ```

# Configuration

There are a number of options that can be specified in your .ini config file.
All configuration options are currently required.

```ini
ckanext.versioned_datastore.elasticsearch_hosts = ...
ckanext.versioned_datastore.elasticsearch_port = ...
ckanext.versioned_datastore.elasticsearch_index_prefix = ...
ckanext.versioned_datastore.mongo_host = ...
ckanext.versioned_datastore.mongo_port = ...
ckanext.versioned_datastore.mongo_database = ...
```

Configuration details:

Name|Description|Example
--|---|--
`ckanext.versioned.datastore.elasticsearch_hosts`|A comma separated list of elasticsearch server hosts|`1.2.3.4,1.5.4.3,es.mydomain.local`
`ckanext.versioned.datastore.elasticsearch_port`|The port for to use for the elasticsearch server hosts listed in the elasticsearch_hosts option|`9200`
`ckanext.versioned.datastore.elasticsearch_index_prefix`|The prefix to use for index names in elasticsearch. Each resource in the datastore gets an index and the name of the index is the resource ID with this prefix prepended.|`nhm-`
`ckanext.versioned.datastore.mongo_host`|The mongo server host|`10.54.24.10`
`ckanext.versioned.datastore.mongo_port`|The port to use to connect to the mongo host|`27017`
`ckanext.versioned.datastore.mongo_database`|The name of the mongo database to use to store datastore data in|`nhm`


# Further Setup

At the version of Eevee this plugin uses, you will also need to:

  - install MongoDB 4.x
  - install Elasticsearch 6.7.x (6.x is probably ok, but untested)

See the [Eevee](https://github.com/NaturalHistoryMuseum/eevee) repository for more details.

This plugin also requires CKAN's job queue, which is included in recent versions of CKAN or can be added to old versions using the ckanext-rq plugin.

# Usage

A brief tour!

The plugin automatically detects resources on upload that can be added to the datastore.
This is accomplished using the resource format.
Currently the accepted formats are:

- CSV - csv, application/csv
- TSV - tsv
- XLS (old excel) - xls, application/vnd.ms-excel
- XLSX (new excel) - xlsx, application/vnd.openxmlformats-officedocument.spreadsheetml.sheet

If one of these formats is used then an attempt will be made to add the uploaded or URL to the datastore.
Note that only the first sheet in multisheet XLS and XLSX files will be processed.

Adding data to the datastore is accomplished in two steps:

1. Ingesting the records into MongoDB. A document is used per unique record ID to store all versions and the documents for a specific resource are stored in a collection named after the resource's ID. For more information on the structure of these documents see the [Eevee](https://github.com/NaturalHistoryMuseum/eevee) repository for more details.
2. Indexing the documents from MongoDB into Elasticsearch. One indexed is used for all versions of the records and a document in Elasticsearch is created per version of each record. The index is named after the resource's ID with the configured prefix prepended. For more information on the structure of these indexed documents see the [Eevee](https://github.com/NaturalHistoryMuseum/eevee) repository for more details.

The ingesting and indexing is completed in the background using the CKAN's job queue.

Once data has been added to the datastore it can be searched using the `datastore_search` or more advanced `datastore_search_raw` actions.
The `datastore_search` action closely mirrors the default CKAN datastore action of the same name.
The `datastore_search_raw` action allows users to query the datastore using raw Elasticsearch queries, unlocking the full range of features it provides.

There are a bunch of other actions available, details of them can be found in the help text for each.

## Action API
Here is a brief overview of the available actions:

  - `datastore_search` - search a resource's data using a similar API to CKAN's default `datastore_search` action
  - `datastore_create` - adds a resource to the versioned datastore (note that this doesn't add any data, it just does setup work. This is different to CKAN's default `datastore_create` action)
  - `datastore_upsert` - upserts data into the datastore for the resource. The data can be provided in the data_dict using the key 'records' or, if data is not specified, the URL on the resource is used
  - `datastore_delete` - deletes the data in the datastore against the given resource ID
  - `datastore_get_record_versions` - given a record id and a resource it appears in, returns the version timestamps available for that record in ascending order
  - `datastore_get_resource_versions` - given a resource id, returns the version timestamps available for that resource in ascending order along with the number of records modified in the version and the number of records at that version
  - `datastore_autocomplete` - provides autocompletion results against a specific field in a specific resource
  - `datastore_reindex` - triggers a reindex of the given resource's data
  - `datastore_query_extent` - return the geospatial extent of the results of a given datastore search query
  - `datastore_get_rounded_version` - round the requested version of this query down to the nearest actual version of the resource
  - `datastore_search_raw` - this action allows you to search data in a resource using a raw elasticsearch query

## Interfaces

One interface is made available through this plugin: `IVersionedDatastore`.

Here is a brief overview of its (fairly poorly named) functions:

  - `datastore_modify_data_dict` - allows modification of the data dict before it is validated and used to create the search object
  - `datastore_modify_search` - allows modifications to the search before it is made. This is kind of analogous to `IDatastore.datastore_search` however instead of passing around a query dict, instead an elasticsearch-dsl `Search` object is passed around
  - `datastore_modify_result` - allows modifications to the result after the search
  - `datastore_modify_fields` - allows modification of the field definitions before they are returned with the results of a datastore_search
  - `datastore_modify_index_doc` - allows the modification of a resource's data during indexing
  - `datastore_is_read_only_resource` - allows implementors to designate certain resources as read only
  - `datastore_after_indexing` - allows implementors to hook onto the completion of an indexing task

See the interface definition in this plugin for more details about these functions.

# Testing

_Test coverage is currently extremely limited._

To run the tests, use nosetests inside your virtualenv. The `--nocapture` flag will allow you to see the debug statements.
```bash
nosetests --ckan --with-pylons=/path/to/your/test.ini --where=/path/to/your/install/directory/ckanext-versioned-datastore --nologcapture --nocapture
```
