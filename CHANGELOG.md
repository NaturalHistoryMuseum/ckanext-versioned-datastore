## v6.5.1 (2026-01-05)

### Fix

- paginate current package list action

## v6.5.0 (2026-01-05)

### Feature

- add action to get field names from latest index
- add sample option to vds_multi_fields

### Fix

- add user key to context even if user is null

### Performance

- check access using current_package_list_with_resources

### Tests

- remove mocking from query args test
- fix validator tests

### Build System(s)

- update splitgill and elasticsearch-dsl

## v6.4.2 (2026-01-02)

### Fix

- stringify list items and filter out nulls

## v6.4.1 (2025-12-29)

### Fix

- serialise lists in dwc-a files as pipe-delimited strings

## v6.4.0 (2025-11-19)

### Feature

- add a flag to disable data ingestion

### Fix

- use API-compatible error for slug reservation
- improve "no slug found" message
- use error compatible with API
- make coordinate field names singular

### Docs

- fix linter error for docstring

### Tests

- update error message
- check for correct error

## v6.3.5 (2025-11-03)

### Fix

- ignore runtime error when adding user to context
- add username to context if empty

## v6.3.4 (2025-08-11)

### Fix

- default to empty string if user not in flask context
- fix summing logic to account for arc indices

### Performance

- check against set of available resources

### Docs

- use relative links for actions docs
- remove references to old actions and add links to new ones
- update endpoint name for openapi docs

### Tests

- use request context in slug/doi tests

## v6.3.3 (2025-07-08)

### Fix

- fix slug import path

## v6.3.2 (2025-06-17)

### Performance

- check resource exists with session instead of action

## v6.3.1 (2025-06-17)

### Fix

- remove auth audit key after failed request
- use get_action rather than check_access

## v6.3.0 (2025-06-16)

### Feature

- add warnings to resolved slugs about missing resources

### Fix

- ignore resources with no database

### Tests

- add tests for slug/doi resolution

## v6.2.1 (2025-06-09)

### Fix

- remove assets entirely

## v6.2.0 (2025-06-09)

### Feature

- allow ES parameters to be passed with search requests

### Fix

- include assets correctly
- ignore missing resources

### Docs

- docstring tidying
- use variable logo, update tests badge

### Style

- remove space
- ruff formatting

### Tests

- remove parenthesised context managers to fix tests
- add some more tests for the search request class

### Build System(s)

- update ckantools

### CI System(s)

- set ruff target py version, add more ignores
- remove pylint, add ruff lint rules Primarily the defaults plus pydocstyle and isort.
- update pre-commit repo versions
- add pull request validation workflow new workflow to check commit format and code style against pre-commit config
- update workflow files standardise format, change name of tests file

### Chores/Misc

- add pull request template
- update tool details in contributing guide

## v6.1.0 (2025-05-27)

### Feature

- infer basic search field response types
- reimplement vds_multi_direct (datastore_search_raw)
- update splitgill to v3.1.0

### Fix

- use max resource version if no requested version specified
- treat types at the threshold as that type
- remove raw_fields from basic response
- handle empty string resource ID list validation
- deduplicate resource IDs when validating them
- rename and redefine custom filename auth function

### Tests

- add infer_type tests
- fix tests

## v6.0.1 (2025-05-06)

### Fix

- remove excel download option
- eat exceptions when attempting to resolve a query doi

### Docs

- remove references to xlsx download in docs

### Tests

- remove excel download option from tests

## v6.0.0 (2025-04-19)

### Breaking Changes

- rename action vds_slug_edit to vds_slug_reserve
- improve vds_version_resource perf by not including counts
- apply compatibility changes to work with Splitgill vNext
- upgrade to es 8+ and mongo 6+, as well as the splitgill vNext branch

### Feature

- upgrade deps
- rename action vds_slug_edit to vds_slug_reserve
- update splitgill version
- update to latest verion of splitgill
- apply compatibility changes to work with Splitgill vNext
- upgrade to es 8+ and mongo 6+, as well as the splitgill vNext branch

### Fix

- rebuild data correctly in downloads
- change more new splitgill version related issues
- update to work with latest version of splitgill
- change package.read to dataset.read in dwc downloader
- create migration for removal of not null constraint on stats version
- use ALL_POINTS for now
- fix basic polygon searches

### Performance

- improve vds_version_resource perf by not including counts

### Docs

- add documentation for all actions
- update some out of date docs

### Tests

- fix the tests

### Build System(s)

- update ckanext-query-dois to new version
- update to newer elasticsearch and mongo versions
- update pre-commit and pyproject
- update splitgill version
- remove no log settings
- rename the test running service test instead of ckan

### CI System(s)

- call correct service in ci
- fix docker compose old style call

### Chores/Misc

- remove debug print statements
- remove empty module
- rename ckan test service from test -> ckan
- remove version from docker compose file
- remove stray file apparently
- show resource id which failed check in validation
- add docker compose override file to gitignore

## v5.6.3 (2024-08-20)

## v5.6.2 (2024-07-15)

### Fix

- increase ttl to 300s
- add cachetools to dependencies
- cache status report functions for 300s

## v5.6.1 (2024-07-08)

### Fix

- increase status tolerance for queue length
- remove hacky queue length estimate

## v5.6.0 (2024-07-08)

### Feature

- add (optional) integration with ckanext-status

### Fix

- use (un)available instead of (dis)connected

## v5.5.0 (2024-03-11)

### Feature

- remove empty groups from multisearch query when running search
- convert single-item OR groups to AND when running search

### Fix

- sort on version rather than date modified

### Refactor

- move normalisation methods into schema.normalise step

### Tests

- add tests for query normalisation utils

## v5.4.0 (2024-01-15)

### Feature

- **downloads**: skip empty extension rows

## v5.3.1 (2023-12-11)

### Fix

- fix a typo and add a space

### Chores/Misc

- add build section to read the docs config

## v5.3.0 (2023-12-04)

### Feature

- add datastore_multisearch_counts action

## v5.2.5 (2023-11-27)

### Fix

- ensure the workbook is closed if something goes wrong during the write call
- request index mappings in batches to avoid elasticsearch URL length error

## v5.2.4 (2023-11-23)

### Fix

- set default resource totals to 0

## v5.2.3 (2023-11-20)

### Fix

- ignore _id fields in extension rows

## v5.2.2 (2023-11-20)

### Fix

- filter out Nones from sum resource count calculation

### Refactor

- set autocomplete result limit to 500

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

## v3.8.0 (2022-12-14)

### Feature

- add more gbif dwc extensions
- add the references extension
- add vernacular name dwc extension

### Fix

- ignore fields that aren't in the dwc schema

## v3.7.2 (2022-12-12)

### Refactor

- move test schemas into data folder

### Docs

- **readme**: add instruction to install lessc globally

### Style

- change quotes in setup.py to single quotes

### Build System(s)

- remove local less installation
- add package data

### Chores/Misc

- merge dev again to try and resolve conflicts

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

### Chores/Misc

- merge/reformat again
- merge package metadata updates from dev

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
