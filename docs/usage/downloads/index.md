Calling the `vds_download_queue` action adds a download job to the queue. There
are four parameters in a request: `query`, `file`, `server`, and `notifier`.

See [Examples](examples) for some complete requests.

## `query`

Describes the search. All fields are optional.

| Term                        | Default                                                               | Description                                                           |
|-----------------------------|-----------------------------------------------------------------------|-----------------------------------------------------------------------|
| `query`                     | `{}`                                                                  | search terms, filters, etc                                            |
| `query_version`             | latest query schema version                                           | the schema your search query uses                                     |
| `version`                   | the latest resource version                                           | a default resource version                                            |
| `resource_ids`              | all available resources                                               | a list of ids of resources to search in                               |
| `resource_ids_and_versions` | the latest version (or `version`) for each resource in `resource_ids` | a dict of resource_id: version. takes precedence over `resource_ids`. |
| `slug_or_doi`               |                                                                       | load a saved query using its memorable slug or doi                    |

e.g.

```json
{
    "query": {
        "filters": {
            "and": [
                {
                    "string_equals": {
                        "fields": [
                            "class"
                        ],
                        "value": "insecta"
                    }
                }
            ]
        }
    },
    "resource_ids": [
        "05ff2255-c38a-40c9-b657-4ccb55ab2feb"
    ]
}
```

***

## `file`

Describes the file being generated in this request (a.k.a. the _derivative_).

`format` is required, but all other fields are optional.

| Term                  | Default | Description                                                                                                                                                                        |
|-----------------------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `format`              |         | the format of the file; see [file formats](options#file-formats)                                                                                                                   |
| `format_args`         | `{}`    | additional parameters/advanced options used by individual formats                                                                                                                  |
| `separate_files`      | False   | separate data from different resources into different files, i.e. one file per resource                                                                                            |
| `ignore_empty_fields` | False   | skip columns where all values are empty                                                                                                                                            |
| `transform`           | {}      | additional data transformations to apply before saving the file, in the format `transformation_name`:`{ option: value }`; see [data transformations](options#data-transformations) |

e.g.

```json
{
    "format": "csv",
    "format_args": {
        "delimiter": "tab"
    },
    "separate_files": true,
    "transform": {
        "id_as_url": {
            "field": "specimenID"
        }
    }
}
```

***

## `server`

Describes the way the file will be served. In this plugin, there is currently only
one `server` type (`direct`), but this can be extended by other plugins.

All fields are optional.

| Term        | Default  | Description                                                            |
|-------------|----------|------------------------------------------------------------------------|
| `type`      | `direct` | the name of the serve method; see [server types](options#server-types) |
| `type_args` | {}       | additional arguments for the server                                    |

e.g.

```json
{
    "type": "direct"
}
```

***

## `notifier`

Describes the way the user will be notified of download progress. The list of available
types can be extended by other plugins.

Field requirements depends on the notifier type selected.

| Term        | Default | Description                                                                   |
|-------------|---------|-------------------------------------------------------------------------------|
| `type`      | `email` | the name of the notifier method; see [notifier types](options#notifier-types) |
| `type_args` | {}      | additional arguments for the notifier                                         |

e.g.

```json
{
    "type": "email",
    "type_args": {
        "emails": [
            "recipient_one@email.com",
            "recipient_two@email.com"
        ]
    }
}
```
