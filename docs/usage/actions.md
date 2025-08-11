# Actions

The most up-to-date documentation for actions is inline:

- [`vds_basic_query`](../../API/versioned_datastore/logic/basic/action/#ckanext.versioned_datastore.logic.basic.action.vds_basic_query)
- [`vds_basic_count`](../../API/versioned_datastore/logic/basic/action/#ckanext.versioned_datastore.logic.basic.action.vds_basic_count)
- [`vds_basic_autocomplete`](../../API/versioned_datastore/logic/basic/action/#ckanext.versioned_datastore.logic.basic.action.vds_basic_autocomplete)
- [`vds_basic_extent`](../../API/versioned_datastore/logic/basic/action/#ckanext.versioned_datastore.logic.basic.action.vds_basic_extent)
- [`vds_resource_check`](../../API/versioned_datastore/logic/resource/action/#ckanext.versioned_datastore.logic.resource.action.vds_resource_check)
- [`vds_version_schema`](../../API/versioned_datastore/logic/version/action/#ckanext.versioned_datastore.logic.version.action.vds_version_schema)
- [`vds_version_record`](../../API/versioned_datastore/logic/version/action/#ckanext.versioned_datastore.logic.version.action.vds_version_record)
- [`vds_version_resource`](../../API/versioned_datastore/logic/version/action/#ckanext.versioned_datastore.logic.version.action.vds_version_resource)
- [`vds_version_round`](../../API/versioned_datastore/logic/version/action/#ckanext.versioned_datastore.logic.version.action.vds_version_round)
- [`vds_data_add`](../../API/versioned_datastore/logic/data/action/#ckanext.versioned_datastore.logic.data.action.vds_data_add)
- [`vds_data_delete`](../../API/versioned_datastore/logic/data/action/#ckanext.versioned_datastore.logic.data.action.vds_data_delete)
- [`vds_data_sync`](../../API/versioned_datastore/logic/data/action/#ckanext.versioned_datastore.logic.data.action.vds_data_sync)
- [`vds_data_get`](../../API/versioned_datastore/logic/data/action/#ckanext.versioned_datastore.logic.data.action.vds_data_get)
- [`vds_multi_query`](../../API/versioned_datastore/logic/multi/action/#ckanext.versioned_datastore.logic.multi.action.vds_multi_query)
- [`vds_multi_count`](../../API/versioned_datastore/logic/multi/action/#ckanext.versioned_datastore.logic.multi.action.vds_multi_count)
- [`vds_multi_autocomplete_value`](../../API/versioned_datastore/logic/multi/action/#ckanext.versioned_datastore.logic.multi.action.vds_multi_autocomplete_value)
- [`vds_multi_autocomplete_field`](../../API/versioned_datastore/logic/multi/action/#ckanext.versioned_datastore.logic.multi.action.vds_multi_autocomplete_field)
- [`vds_multi_hash`](../../API/versioned_datastore/logic/multi/action/#ckanext.versioned_datastore.logic.multi.action.vds_multi_hash)
- [`vds_multi_fields`](../../API/versioned_datastore/logic/multi/action/#ckanext.versioned_datastore.logic.multi.action.vds_multi_fields)
- [`vds_multi_stats`](../../API/versioned_datastore/logic/multi/action/#ckanext.versioned_datastore.logic.multi.action.vds_multi_stats)
- [`vds_multi_direct`](../../API/versioned_datastore/logic/multi/action/#ckanext.versioned_datastore.logic.multi.action.vds_multi_direct)
- [`vds_options_get`](../../API/versioned_datastore/logic/options/action/#ckanext.versioned_datastore.logic.options.action.vds_options_get)
- [`vds_options_update`](../../API/versioned_datastore/logic/options/action/#ckanext.versioned_datastore.logic.options.action.vds_options_update)
- [`vds_download_queue`](../../API/versioned_datastore/logic/download/action/#ckanext.versioned_datastore.logic.download.action.vds_download_queue)
- [`vds_download_regenerate`](../../API/versioned_datastore/logic/download/action/#ckanext.versioned_datastore.logic.download.action.vds_download_regenerate)
- [`vds_schema_latest`](../../API/versioned_datastore/logic/schema/action/#ckanext.versioned_datastore.logic.schema.action.vds_schema_latest)
- [`vds_schema_validate`](../../API/versioned_datastore/logic/schema/action/#ckanext.versioned_datastore.logic.schema.action.vds_schema_validate)
- [`vds_slug_create`](../../API/versioned_datastore/logic/slug/action/#ckanext.versioned_datastore.logic.slug.action.vds_slug_create)
- [`vds_slug_resolve`](../../API/versioned_datastore/logic/slug/action/#ckanext.versioned_datastore.logic.slug.action.vds_slug_resolve)
- [`vds_slug_reserve`](../../API/versioned_datastore/logic/slug/action/#ckanext.versioned_datastore.logic.slug.action.vds_slug_reserve)

## Differences from pre-v6 API

For [version 6](https://github.com/NaturalHistoryMuseum/ckanext-versioned-datastore/releases/tag/v6.0.0) of this extension, all of the actions were renamed and many were extensively modified.

This page is to help map the old actions to the closest equivalent new one.

| v5 name                                     | v6 name                        | notes                                                                |
|---------------------------------------------|--------------------------------|----------------------------------------------------------------------|
| `datastore_autocomplete`                    | `vds_basic_autocomplete`       |                                                                      |
| `datastore_count`                           | `vds_basic_count`              |                                                                      |
| `datastore_create_slug`                     | `vds_slug_create`              |                                                                      |
| `datastore_create`                          | `vds_data_add`                 | `_create` and `_upsert` were combined                                |
| `datastore_delete`                          | `vds_data_delete`              |                                                                      |
| `datastore_edit_slug`                       | `vds_slug_reserve`             |                                                                      |
| `datastore_ensure_privacy`                  | --                             |                                                                      |
| `datastore_field_autocomplete`              | `vds_multi_autocomplete_field` |                                                                      |
| `datastore_get_latest_query_schema_version` | `vds_schema_latest`            |                                                                      |
| `datastore_get_record_versions`             | `vds_version_record`           |                                                                      |
| `datastore_get_resource_versions`           | `vds_version_resource`         |                                                                      |
| `datastore_get_rounded_version`             | `vds_version_round`            |                                                                      |
| `datastore_guess_fields`                    | `vds_multi_fields`             |                                                                      |
| `datastore_hash_query`                      | `vds_multi_hash`               |                                                                      |
| `datastore_is_datastore_resource`           | `vds_resource_check`           |                                                                      |
| `datastore_multisearch_counts`              | `vds_multi_count`              |                                                                      |
| `datastore_multisearch`                     | `vds_multi_query`              |                                                                      |
| `datastore_query_extent`                    | `vds_basic_extent`             | `datastore_query_extent` still exists, but just calls the new action |
| `datastore_queue_download`                  | `vds_download_queue`           |                                                                      |
| `datastore_regenerate_download`             | `vds_download_regenerate`      |                                                                      |
| `datastore_reindex`                         | `vds_data_sync`                |                                                                      |
| `datastore_resolve_slug`                    | `vds_slug_resolve`             |                                                                      |
| `datastore_search_raw`                      | `vds_multi_direct`             |                                                                      |
| `datastore_search`                          | `vds_basic_query`              | `datastore_search` still exists, but it just calls `vds_basic_query` |
| `datastore_upsert`                          | `vds_data_add`                 | `_create` and `_upsert` were combined                                |
| `datastore_value_autocomplete`              | `vds_multi_autocomplete_value` |                                                                      |
| --                                          | `vds_data_get`                 |                                                                      |
| --                                          | `vds_multi_stats`              |                                                                      |
| --                                          | `vds_options_get`              |                                                                      |
| --                                          | `vds_options_update`           |                                                                      |
| --                                          | `vds_schema_validate`          |                                                                      |
| --                                          | `vds_version_schema`           |                                                                      |
