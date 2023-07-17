<!--header-start-->
<img src="https://data.nhm.ac.uk/images/nhm_logo.svg" align="left" width="150px" height="100px" hspace="40"/>

# ckanext-versioned-datastore

[![Tests](https://img.shields.io/github/actions/workflow/status/NaturalHistoryMuseum/ckanext-versioned-datastore/main.yml?style=flat-square)](https://github.com/NaturalHistoryMuseum/ckanext-versioned-datastore/actions/workflows/main.yml)
[![Coveralls](https://img.shields.io/coveralls/github/NaturalHistoryMuseum/ckanext-versioned-datastore/main?style=flat-square)](https://coveralls.io/github/NaturalHistoryMuseum/ckanext-versioned-datastore)
[![CKAN](https://img.shields.io/badge/ckan-2.9.7-orange.svg?style=flat-square)](https://github.com/ckan/ckan)
[![Python](https://img.shields.io/badge/python-3.6%20%7C%203.7%20%7C%203.8-blue.svg?style=flat-square)](https://www.python.org/)
[![Docs](https://img.shields.io/readthedocs/ckanext-versioned-datastore?style=flat-square)](https://ckanext-versioned-datastore.readthedocs.io)

_A CKAN extension providing a versioned datastore using MongoDB and Elasticsearch_

<!--header-end-->

# Overview

<!--overview-start-->
This plugin provides a complete replacement for ckan's datastore plugin and therefore shouldn't be used in conjunction with it.
Rather than storing data in PostgreSQL, resource data is stored in MongoDB and then made available to frontend APIs using Elasticsearch.

This allows this plugin to:

  - provide full versioning of resource records - records can be updated when new resource data is uploaded without preventing access to the old data
  - expose advanced search features using Elasticsearch's extensive feature set
  - achieve fast search response times, particularly when compared to PostgreSQL, due Elasticsearch's search performance
  - store large resources (millions of rows) and still provide high speed search responses
  - store complex data as both MongoDB and Elasticsearch are JSON based, allowing object nesting and arrays

This plugin is built on [Splitgill](https://github.com/NaturalHistoryMuseum/splitgill).

<!--overview-end-->

# Installation

<!--installation-start-->
Path variables used below:
- `$INSTALL_FOLDER` (i.e. where CKAN is installed), e.g. `/usr/lib/ckan/default`
- `$CONFIG_FILE`, e.g. `/etc/ckan/default/development.ini`

## Installing from PyPI

```shell
pip install ckanext-versioned-datastore
```

## Installing from source

1. Clone the repository into the `src` folder:
   ```shell
   cd $INSTALL_FOLDER/src
   git clone https://github.com/NaturalHistoryMuseum/ckanext-versioned-datastore.git
   ```

2. Activate the virtual env:
   ```shell
   . $INSTALL_FOLDER/bin/activate
   ```

3. Install via pip:
   ```shell
   pip install $INSTALL_FOLDER/src/ckanext-versioned-datastore
   ```

### Installing in editable mode

Installing from a `pyproject.toml` in editable mode (i.e. `pip install -e`) requires `setuptools>=64`; however, CKAN 2.9 requires `setuptools==44.1.0`. See [our CKAN fork](https://github.com/NaturalHistoryMuseum/ckan) for a version of v2.9 that uses an updated setuptools if this functionality is something you need.

## Post-install setup

1. Add 'versioned_datastore' to the list of plugins in your `$CONFIG_FILE`:
   ```ini
   ckan.plugins = ... versioned_datastore
   ```

2. Install `lessc` globally:
   ```shell
   npm install -g "less@~4.1"
   ```

## Other requirements

At the version of [splitgill](https://github.com/NaturalHistoryMuseum/splitgill) this plugin uses, you will also need to install:
  - MongoDB 4.x
  - Elasticsearch 6.7.x (6.x is probably ok, but untested)

This plugin also requires CKAN's job queue, which is included in recent versions of CKAN or can be added to older versions using the ckanext-rq plugin.

<!--installation-end-->

# Configuration

<!--configuration-start-->
There are a number of options that can be specified in your .ini config file.
All configuration options are currently required.

## **[REQUIRED]**

| Name                                                     | Description                                                                                                                                                               | Example                             |
|----------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------|
| `ckanext.versioned_datastore.elasticsearch_hosts`        | A comma separated list of elasticsearch server hosts                                                                                                                      | `1.2.3.4,1.5.4.3,es.mydomain.local` |
| `ckanext.versioned_datastore.elasticsearch_port`         | The port for to use for the elasticsearch server hosts listed in the elasticsearch_hosts option                                                                           | `9200`                              |
| `ckanext.versioned_datastore.elasticsearch_index_prefix` | The prefix to use for index names in elasticsearch. Each resource in the datastore gets an index and the name of the index is the resource ID with this prefix prepended. | `nhm-`                              |
| `ckanext.versioned_datastore.mongo_host`                 | The mongo server host                                                                                                                                                     | `10.54.24.10`                       |
| `ckanext.versioned_datastore.mongo_port`                 | The port to use to connect to the mongo host                                                                                                                              | `27017`                             |
| `ckanext.versioned_datastore.mongo_database`             | The name of the mongo database to use to store datastore data in                                                                                                          | `nhm`                               |

## **[OPTIONAL]**

| Name                                                  | Description                                                                                                                                        | Example                                                      |
|-------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------|
| `ckanext.versioned_datastore.redis_host`              | The redis server host. If this is provided slugging is enabled                                                                                     | `14.1.214.50`                                                |
| `ckanext.versioned_datastore.redis_port`              | The port to use to connect to the redis host                                                                                                       | `6379`                                                       |
| `ckanext.versioned_datastore.redis_database`          | The redis database index to use to store datastore multisearch slugs in                                                                            | `1`                                                          |
| `ckanext.versioned_datastore.slug_ttl`                | The amount of time slugs should last for, in days. Default: `7`                                                                                    | `7`                                                          |
| `ckanext.versioned_datastore.dwc_core_extension_name` | The name of the DwC core extension to use, as defined in [dwc/writer.py](/ckanext/versioned_datastore/lib/downloads/dwc/writer.py).                | `gbif_occurrence`                                            |
| `ckanext.versioned_datastore.dwc_extension_names`     | A comma-separated list of (non-core) DwC extension names, as defined in [dwc/writer.py](/ckanext/versioned_datastore/lib/downloads/dwc/writer.py). | `gbif_multimedia`                                            |
| `ckanext.versioned_datastore.dwc_org_name`            | The organisation name to use in DwC-A metadata. Default: the value of `ckanext.doi.publisher` or `ckan.site_title`                                 | `The Natural History Museum`                                 |
| `ckanext.versioned_datastore.dwc_org_email`           | The contact email to use in DwC-A metadata. Default: the value of `smtp.mail_from`                                                                 | `contact@yoursite.com`                                       |
| `ckanext.versioned_datastore.dwc_default_license`     | The license to use in DwC-A metadata if the resources have differing licenses or no license is specified. Default: `null`                          | `http://creativecommons.org/publicdomain/zero/1.0/legalcode` |

<!--configuration-end-->

# Usage

<!--usage-start-->
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

1. Ingesting the records into MongoDB. A document is used per unique record ID to store all versions and the documents for a specific resource are stored in a collection named after the resource's ID. For more information on the structure of these documents see the [Splitgill](https://github.com/NaturalHistoryMuseum/splitgill) repository for more details.
2. Indexing the documents from MongoDB into Elasticsearch. One indexed is used for all versions of the records and a document in Elasticsearch is created per version of each record. The index is named after the resource's ID with the configured prefix prepended. For more information on the structure of these indexed documents see the [Splitgill](https://github.com/NaturalHistoryMuseum/splitgill) repository for more details.

The ingesting and indexing is completed in the background using the CKAN's job queue.

Once data has been added to the datastore it can be searched using the `datastore_search` or more advanced `datastore_search_raw` actions.
The `datastore_search` action closely mirrors the default CKAN datastore action of the same name.
The `datastore_search_raw` action allows users to query the datastore using raw Elasticsearch queries, unlocking the full range of features it provides.

## Actions

All of this extension's actions are fully documented inline, including all parameters and results.

### `datastore_create`
Adds a resource to the versioned datastore (note that this doesn't add any data, it just does setup work. This is different to CKAN's default `datastore_create` action).

### `datastore_upsert`
Upserts data into the datastore for the resource. The data can be provided in the data_dict using the key 'records' or, if data is not specified, the URL on the resource is used.

### `datastore_delete`
Deletes the data in the datastore against the given resource ID.

### `datastore_search`
Search a resource's data using a similar API to CKAN's default `datastore_search` action.

### `datastore_get_record_versions`
Given a record id and a resource it appears in, returns the version timestamps available for that record in ascending order.

### `datastore_get_resource_versions`
Given a resource id, returns the version timestamps available for that resource in ascending order along with the number of records modified in the version and the number of records at that version.

### `datastore_autocomplete`
Provides autocompletion results against a specific field in a specific resource.

### `datastore_reindex`
Triggers a reindex of the given resource's data.

### `datastore_query_extent`
Return the geospatial extent of the results of a given datastore search query.

### `datastore_get_rounded_version`
Round the requested version of this query down to the nearest actual version of the resource.

### `datastore_search_raw`
This action allows you to search data in a resource using a raw elasticsearch query.

### `datastore_ensure_privacy`
This action runs through all resources (or handles a specific resource if a resource id is provided) and makes sure that the privacy set on each resource's package is reflected in the datastore.

## Commands

### `vds`

1. `initdb`: ensure the tables needed by this plugin exist.
    ```bash
    ckan -c $CONFIG_FILE initdb
    ```

2. `reindex`: reindex either a specific resource or all resources.
    ```bash
    ckan -c $CONFIG_FILE reindex $OPTIONAL_RESOURCE_ID
    ```

## Interfaces

### `IVersionedDatastore`
This is the most general interface.

Here is a brief overview of its functions:

  - `datastore_modify_data_dict` - allows modification of the data dict before it is validated and used to create the search object
  - `datastore_modify_search` - allows modifications to the search before it is made. This is kind of analogous to `IDatastore.datastore_search` however instead of passing around a query dict, instead an elasticsearch-dsl `Search` object is passed around
  - `datastore_modify_result` - allows modifications to the result after the search
  - `datastore_modify_fields` - allows modification of the field definitions before they are returned with the results of a datastore_search
  - `datastore_modify_index_doc` - allows the modification of a resource's data during indexing
  - `datastore_is_read_only_resource` - allows implementors to designate certain resources as read only
  - `datastore_after_indexing` - allows implementors to hook onto the completion of an indexing task

See the interface definition in this plugin for more details about these functions.

### `IVersionedDatastoreQuery`
This interface handles hooks and functions specifically relating to search queries.

  - `get_query_schemas` - allows registering custom query schemas

### `IVersionedDatastoreDownloads`
This interface handles hooks and functions specifically relating to downloads.

  - `download_modify_notifier_start_templates` - modify the templates used when sending notifications that a download has started
  - `download_modify_notifier_end_templates` - modify the templates used when sending notifications that a download has ended
  - `download_modify_notifier_error_templates` - modify the templates used when sending notifications that a download has failed
  - `download_modify_notifier_template_context` - modify the context/variables used to populate the notification templates
  - `download_derivative_generators` - extend or modify the list of derivative generators
  - `download_file_servers` - extend or modify the list of file servers
  - `download_notifiers` - extend or modify the list of notifiers
  - `download_data_transformations` - extend or modify the list of data transformations
  - `download_modify_manifest` - modify the manifest included in the download file
  - `download_before_run` - modify args before any search is run or files generated
  - `download_after_run` - hook notifying that a download has finished (whether failed or completed)

<!--usage-end-->

# Testing

<!--testing-start-->
There is a Docker compose configuration available in this repository to make it easier to run tests. The ckan image uses the Dockerfile in the `docker/` folder.

To run the tests against ckan 2.9.x on Python3:

1. Build the required images:
   ```shell
   docker-compose build
   ```

2. Then run the tests.
   The root of the repository is mounted into the ckan container as a volume by the Docker compose
   configuration, so you should only need to rebuild the ckan image if you change the extension's
   dependencies.
   ```shell
   docker-compose run ckan
   ```

<!--testing-end-->
