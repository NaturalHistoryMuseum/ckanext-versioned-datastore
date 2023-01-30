## 1. Minimal

The most basic, barebones request you can get away with:

```json
{
    "file": {
        "format": "csv"
    },
    "notifier": {
        "type": "none"
    }
}
```

If successful, this will return a `download_id` in the response. You can then
visit `https://SITE_URL/status/download/DOWNLOAD_ID` to monitor progress and eventually
download your generated file.

## 2. Download a saved search

The search results are separate to the file, so you can apply any file arguments you
like to an existing search.

```json
{
    "query": {
        "slug_or_doi": "noisy-yelling-cat"
    },
    "file": {
        "format": "xlsx",
        "ignore_empty_fields": true
    },
    "notifier": {
        "type": "email",
        "type_args": {
            "emails": [
                "your@email.here"
            ]
        }
    }
}
```

This will send status updates to `your@email.here`.

## 3. A complex request

Let's say you want to search across specific versions of three of your resources and
download the records that are either of a plant in Africa or an animal in Spain (I don't
know why, I don't know your life). You want the file in Darwin Core format with some
specific extensions enabled. You also want to send progress updates to an external URL,
and this URL requires that text is `POST`ed to it in a parameter named `TXT`.

```json
{
    "query": {
        "query": {
            "filters": {
                "and": [
                    {
                        "or": [
                            {
                                "and": [
                                    {
                                        "string_equals": {
                                            "fields": [
                                                "kingdom"
                                            ],
                                            "value": "plantae"
                                        }
                                    },
                                    {
                                        "string_equals": {
                                            "fields": [
                                                "continent",
                                                "Continent",
                                                "CONTINENT"
                                            ],
                                            "value": "africa"
                                        }
                                    }
                                ]
                            },
                            {
                                "and": [
                                    {
                                        "string_equals": {
                                            "fields": [
                                                "kingdom"
                                            ],
                                            "value": "animalia"
                                        }
                                    },
                                    {
                                        "string_equals": {
                                            "fields": [
                                                "country"
                                            ],
                                            "value": "spain"
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        },
        "resource_ids_and_versions": {
            "resource-id-one": 123456789,
            "resource-id-two": 123456785,
            "resource-id-three": 123456780
        }
    },
    "file": {
        "format": "dwc",
        "ignore_empty_fields": true,
        "format_args": {
            "core_extension_name": "gbif_taxon",
            "extension_names": [
                "gbif_multimedia"
            ]
        }
    },
    "notifier": {
        "type": "webhook",
        "type_args": {
            "url": "https://your-url-here.com",
            "text_param": "TXT",
            "post": true
        }
    }
}
```
