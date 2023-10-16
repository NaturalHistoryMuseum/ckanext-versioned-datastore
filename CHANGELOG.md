# Changelog

## v5.2.1 (2023-10-16)

### Fix

- use msearches to increase datastore_get_resource_versions performance

### Refactor

- rename a variable to something better

### Performance

- increase msearch batch size from 10 to 100

### Docs

- update docs to correctly detail return type

## v5.2.0 (2023-10-05)

### Feature

- add download_after_init hook

### Fix

- use the correct number when checking resource and package list len

### Tests

- add new hook to mock plugins

## v5.1.1 (2023-10-05)

### Fix

- refactor and fix eml generation

## v5.1.0 (2023-10-02)

### Feature

- allow custom titles on dwc downloads of queries

### Fix

- try two different debug variable names and cast to bool

### Chores/Misc

- add regex for version line in citation file
- add citation.cff to list of files with version
- add contributing guidelines
- add code of conduct
- add citation file
- update support.md links

## v5.0.2 (2023-07-20)

### Fix

- remove the text saying multiple emails are allowed
- make email required and add validation in popup
- add validators for notifiers (email and webhook)
- put notifier call inside try/except

## v5.0.1 (2023-07-18)

### Fix

- init nav slugs table in cli

## v5.0.0 (2023-07-18)

### Breaking Changes

- remove request id from file names

### Feature

- add action to regenerate downloads
- add search url and check for file on status page
- add json endpoint for download status, add urls to queue action
- allow admin users to specify a custom filename for downloads
- add server_args to request model
- add new interface method to modify eml
- allow other plugins to modify converted queries
- add a 'raw' download option that also allows non-datastore files
- allow setting the download query from the url
- convert to multisearch if query_version starts with 'v0'
- add helper for creating nav slugs in templates
- add navigational slugs
- create util method for getting lists of resource ids and versions
- convert old-style queries to new multisearch queries
- add slug_or_doi option to button
- allow adding a json search query to the download button
- add download button snippet for single resources

### Fix

- don't return search url for non-datastore resources
- assume file doesn't exist if filepath is None
- ignore missing server and notifier types
- change default notifier to none
- add raw state to status response
- get the correct debug variable
- use filename instead of temporary filepath for core file
- convert xml content to string
- add geo coverage to eml generation
- use jinja's none, not python's None
- don't process zero-length filters
- show 0 record totals
- add pub/sub to the download button to close if another is opened
- set datastore template option true by default
- set values to true/false instead of on/off
- add error message for missing storage path config
- allow for non-existent resource id list
- convert old queries when resolving slugs
- load geo json if passed as a string
- open search in new page
- preload main js before download button js

### Refactor

- remove request id from file names

### Tests

- mock url_for in query dois test
- patch url_for in direct call test
- use download hash filename
- patch url_for
- add test for non-datastore resource downloads
- add storage path to config
- fix path to get_available_datastore_resources
- add nav slugs table to test setup

## v4.2.2 (2023-07-17)

### Fix

- catch null values in xlsx files better

### Docs

- update logos

## v4.2.1 (2023-04-11)

### Build System(s)

- fix postgres not loading when running tests in docker

### Chores/Misc

- add action to sync branches when commits are pushed to main

## v4.2.0 (2023-03-06)

### Feature

- add helpers for displaying recent versions and readable dates

## v4.1.1 (2023-02-20)

### Docs

- fix api docs generation script

### Style

- reformat with prettier

### Chores/Misc

- small fixes to align with other extensions

## v4.1.0 (2023-02-06)

### Feature

- scroll long queries on download status page

### Minor UI Changes

- add refresh button to status page

## v4.0.3 (2023-01-31)

### Docs

- **readme**: change blob url to raw

## v4.0.2 (2023-01-31)

### Docs

- **readme**: direct link to logo in readme
- **readme**: fix github actions badge

## v4.0.1 (2023-01-31)

### Fix

- strip the extra stuff from the email template

## v4.0.0 (2023-01-30)

### Breaking Changes

- complete refactor of download system.

### Feature

- add new interface hook that runs after the download finishes
- link doi directly to landing page
- include current stage in error message
- add doi to download status page
- **interfaces**: allow plugins to modify download args and manifest
- add button styling to download link
- improve layout of download status page

### Fix

- **downloads**: refresh database models created in another session
- stop excluding the record with the information
- load transforms correctly
- add derivative_gen status update to beginning of section
- use FileNotFoundError when removing temp files
- handle downloads where resource details are not available
- remove cached package from context
- remove temp files when download finishes
- use update() method when updating status
- show custom error message if download cannot be found
- set rq timeout for downloads to 24h
- allow objects to be null in avro schema
- get a different schema for each resource + add more nesting
- get detail for existing resources first
- return a better error message
- only search for records containing the resource count
- return zero count if resource hasn't been counted yet
- commit changes instead of saving if instance already in session
- eager-load download relationships
- **download-status**: only show resources with records
- **migrations**: use .first() because there may be multiple records
- **downloads**: don't return none if the downloads folder doesn't exist
- return tuple from check_for_records
- initialise core and derivative records earlier
- **downloads**: exclude resources with 0 results
- add alt text for total if None
- **migration**: add defaults for file options
- **derivatives**: add setup method to refresh writers
- spell xlsx correctly
- use makedirs to make intermediate folders if necessary
- remove LESS_BIN for download-status-css

### Refactor

- add type hints to datastore_queue_download
- move fixture into conftest.py
- move tests into new folder structure
- move test schemas into data folder

### Docs

- add a notice about v4 to the readme
- add more interface methods to the readme
- fix the migration filter
- make docs generation compatible with py <3.9
- add usage docs for downloads
- remove duplicate param in docstring
- exclude migration dir from API docs generation
- enable permalinks
- fix some comment wrapping
- add docstring for get_schema

### Style

- fix comment line lengths

### Tests

- revert changes to test_downloads_runmanager
- add new interface method for tests
- fix get_schemas test
- add tests for download plugin hooks
- add more reusable patch methods
- **helpers**: set scope for vds tables fixture to class
- patch schemas, don't overwrite them
- **downloads**: patch get_rounded_versions not SEARCH_HELPER
- add tests for flatten_dict
- add tests for filter_data_fields
- add another resource to the test data, test ignore_empty properly
- set emptyField to none
- **downloads**: add more checks for download file content
- **helpers**: move test data into separate file
- **downloads**: add additional checks to download tests
- **downloads**: add tests for dwc schema serialisation
- **downloads**: add tests for notifiers
- **downloads**: patch some parts of query_dois to test some dwc logic
- clear download dir before running tests
- **downloads**: patch url_for
- **helpers**: give an exception if wait time exceeded
- **helpers**: remove table dropping
- **downloads**: add test for integration with query-dois
- **downloads**: patch enqueue_job for the whole class
- **helpers**: drop vds tables at end of test
- **downloads**: add test for transformations
- **downloads**: add more scenarios for download integration tests
- **helpers**: yield created resource dict from vds resource fixture
- **helpers**: make es/mongo clear fixture a teardown method
- patch get_available_datastore_resources again
- fix query test
- **helpers**: add fixture to clear mongo and es
- wait for data to be added to datastore
- omit format arg when creating data, specify url
- **helpers**: add sync enqueue mock that uses a thread
- **downloads**: replace elasticsearch_scan fixture with patches
- **downloads**: remove old download tests
- **downloads**: remove unnecessary ES patches and duplicate tests
- **downloads**: add tests for creating queries
- add patch helpers for simple common patches
- **downloads**: add test for download with filter query
- **downloads**: add test for get_schema
- **downloads**: add test that runs a basic download
- **downloads**: add new tests for datastore_queue_download

### Build System(s)

- add migration dir to package-data
- **migration**: add migration script for new download models
- **docker**: use latest ckantest image tag
- add query-dois and attribution as optional dependencies
- source the test script
- add fastavro dependency

### CI System(s)

- use bash not source, bump docker image version
- source the test script in github actions too

### Chores/Misc

- remove incorrect tag
- rename some tests
- merge in new changes from dev
- merge dev again to try and resolve conflicts
- merge/reformat again
- merge package metadata updates from dev

## v3.8.0 (2022-12-14)

### Feature

- add more gbif dwc extensions
- add the references extension
- add vernacular name dwc extension

### Fix

- ignore fields that aren't in the dwc schema

## v3.7.2 (2022-12-12)

### Docs

- **readme**: add instruction to install lessc globally

### Style

- change quotes in setup.py to single quotes

### Build System(s)

- remove local less installation
- add package data

## v3.7.1 (2022-12-01)

### Docs

- **readme**: fix table borders
- **readme**: format test section
- **readme**: update installation steps
- **readme**: update ckan patch version in header badge

## v3.7.0 (2022-11-28)

### Fix

- splitgill, not eevee

### Docs

- add section delimiters and include-markdown

### Style

- apply formatting

### Build System(s)

- set changelog generation to incremental
- pin ckantools minor

### CI System(s)

- add cz-nhm dependency

### Chores/Misc

- remove manifest
- use cz_nhm commitizen config
- standardise package files

## v3.6.2.1 (2022-10-13)

## v3.6.2 (2022-10-03)

## v3.6.1 (2022-09-06)

## v3.6.0 (2022-08-30)

## v3.5.2 (2022-08-22)

## v3.5.1 (2022-08-08)

## v3.5.0 (2022-05-23)

## v3.4.0 (2022-05-03)

## v3.3.0 (2022-04-25)

## v3.2.1 (2022-03-28)

## v3.2.0 (2022-03-21)

## v3.1.3 (2022-03-10)

## v3.1.2 (2022-03-08)

## v3.1.1 (2022-03-07)

## v3.1.0 (2022-02-28)

## v3.0.0 (2021-03-11)

## v2.0.0 (2021-03-09)

## v0.0.5 (2019-08-07)

## v1.0.0-alpha (2019-07-23)

## v0.0.4 (2019-05-31)

## v0.0.3 (2019-05-31)

## v0.0.2 (2019-05-02)

## v0.0.1 (2019-04-18)
