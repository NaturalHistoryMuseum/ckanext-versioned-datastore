## File formats

This lists the formats included in _this_ extension: other plugins may extend or
override this list.

### CSV

Comma-separated value format.

| Name        | Options                    |
|-------------|----------------------------|
| `delimiter` | `comma` (default) or `tab` |

```json
{
    "format": "csv",
    "format_args": {
        "delimiter": "tab"  // optional
    }
}
```

### Excel spreadsheet

A Microsoft Excel spreadsheet.

```json
{
    "format": "xlsx"
    // no additional options
}
```

### JSON

JSON format.

```json
{
    "format": "json"
    // no additional options
}
```

### Darwin Core

A [Darwin Core Archive](https://dwc.tdwg.org).

| Name                  | Options                                                                                                                                                                | Notes                                                                                                                                                                                                                                                                   |
|-----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `core_extension_name` | `gbif_occurrence`, `gbif_taxon`, `gbif_event`                                                                                                                          | default can be set in config as `ckanext.versioned_datastore.dwc_core_extension_name`                                                                                                                                                                                   |
| `extension_names`     | `gbif_multimedia`, `gbif_vernacular`, `gbif_references`, `gbif_description`, `gbif_distribution`, `gbif_species_profile`, `gbif_types_and_specimen`, `gbif_identifier` | a list of any/all/none of these names; default can be set in config as `ckanext.versioned_datastore.dwc_extension_names`                                                                                                                                                |
| `extension_map`       |                                                                                                                                                                        | overrides the default fields set in `ckanext/versioned_datastore/lib/downloads/derivatives/dwc/urls.py`; e.g. if your multimedia information is in a field called `imgs` rather than `associatedMedia`, you would use: `"extension_map": {"gbif_multimedia": ["imgs"]}` |
| `id_field`            | any valid field                                                                                                                                                        | the name of the field containing the ID for each record                                                                                                                                                                                                                 |

```json
{
    "format": "dwc",
    "format_args": {
        "core_extension_name": "gbif_occurrence",  // optional
        "extension_names": ["gbif_multimedia", "gbif_vernacular"],  // optional
        "extension_map": {
            "gbif_multimedia": ["imgs"]
        },  // optional
        "id_field": "specimenID"  // optional
    }
}
```

## Data transformations

This lists the transformation functions included in _this_ extension: other plugins may
extend or override this list.

### ID as URL

Reformats an ID field to display as a URL,
e.g. `specimenID: 1234` -> `specimenID: https://SITE_URL/specimen/1234`.

Requires the CKAN instance to have an endpoint/route that builds a URL based on this ID.
By default it will use `object.view`, but the endpoint name can be specified explicitly
by setting the config option `ckanext.versioned_datastore.record_view_endpoint`.

```json
{
    "transform": {
        "id_as_url": {
            "field": "specimenID"
        }
    }
}
```

## Server types

This lists the server types included in _this_ extension: other plugins may extend or
override this list.

### Direct

Serves the file directly over HTTP. This is the default.

```json
{
    "type": "direct"
}
```

## Notifier types

This lists the notifier types included in _this_ extension: other plugins may extend or
override this list.

### Null/none

No notifications; the user must check the status page manually.

```json
{
    "type": "none"
    // no additional options
}
```

### Email

Send progress updates to one or more email addresses.

```json
{
    "type": "email",
    "type_args": {
        "emails": ["your@email.here"]
    }
}
```

### Webhook

Send progress updates to an external webhook.

```json
{
    "type": "webhook",
    "type_args": {
            "url": "https://your-url-here.com",
            "text_param": "text_parameter_name",  // optional, default "text"
            "url_param": "url_parameter_name",  // optional, default "url"
            "post": true  // optional, default false
        }
}
```

***
