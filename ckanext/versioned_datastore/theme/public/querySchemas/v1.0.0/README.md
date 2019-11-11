# Data Portal Query Schema

## Introduction
The files in this folder represent the various versions of the Data Portal's datastore search
schema.
Each schema is named after the version it describes (e.g. v1.0.0) and follows semantic versioning.
The `exampleQueries` directory contains example queries.

## Schemas
### v1.0.0
#### search
At the root of the query object a `search` can be provided, like so:

```json
{
  "search": "mollusca"
}
```
This searches the given value against all fields (using the `meta.all` field).
Using this key in conjunction with the `filters` key will produce an `and` query where the `search`
and the `filters` must be met by result records.

Using this search key is equivalent to using a `string_contains` with `fields: []`.
If you want to create more complicated queries using `groups` with free text searches, use
`string_contains` terms in a `group`.

#### filters
This key defines the top level `group` to be used to filter the results.
Only one `group` can exist at the top level but it can contain other `groups` to create sub
`groups`.

##### Groups
`groups` wrap `terms` and other `groups` allowing the encapsulation of boolean logic (`and`, `or`
and `not`).
Within a group, there are 3 available types: `and`, `or` and `not`.

###### and
The `and` type ensures that **all** terms and groups in the array must be met by the records being
queried. Only records that meet all the constraints will be returned in the results.

```json
{
    "and": [
      ...
    ]
}
```

###### or
The `or` ensures that **at least one** of the terms or groups in the array are met by the records
being queried. Only records that meet **at least one** of the constraints will be returned in the
results.

```json
{
    "or": [
      ...
    ]
}
```

###### not
The `not` type ensures that **all** terms and groups in the array _must not_ be met by the records
being queried. Only records that meet none of the constraints will be returned in the results.

```json
{
    "not": [
      ...
    ]
}
```


##### Terms
###### string_equals
This term matches a string value exactly.
The match is always case insensitive.
```json
{
  ...
  {
    "string_equals": {
      "fields": [
        "genus"
      ],
      "value": "helix"
    }
  }
  ...
}
```

###### string_contains
This term matches a string value that contains the given value.
If more than one word is included in the value then all words must match in a record to be counted
as a match.
The match is always case insensitive.

To search on all fields instead of a selection use an empty array `[]` as the value of the `fields`
key.

```json
{
  ...
  {
    "string_contains": {
      "fields": [
        "country"
      ],
      "value": "united kingdom"
    }
  }
  ...
}
```

###### number_equals
This term matches a number value exactly.

```json
{
  ...
  {
    "number_equals": {
      "fields": [
        "length"
      ],
      "value": "45.19"
    }
  }
  ...
}
```

###### number_range
This term matches a number within the given range.
Inclusivity at the start and end of the range can be optionally controlled using the
`less_than_inclusive` and `greater_than_inclusive` options which both default to `true`.

```json
{
  ...
  {
    "number_range": {
      "fields": [
        "year"
      ],
      "greater_than": 1974,
      "less_than": 2005,
      "less_than_inclusive": false,
    }
  }
  ...
}
```

###### geo_point
This term matches a coordinate pair exactly or within a given circular area.
No `fields` key is necessary as the `meta.geo` field is always used.

If a `radius` is provided, then a `radius_unit` must also be provided.
The available `radius_units` are:

    - `"mi"` - miles
    - `"yd"` - yards
    - `"ft"` - feet
    - `"in"` - inches
    - `"km"` - kilometers
    - `"m"` - metres
    - `"cm"` - centimetres
    - `"mm"` - millimeters
    - `"nmi"` - nautical miles

The `radius` must be greater than or equal to 0.

```json
{
  ...
  {
    "geo_point": {
      "latitude": 51.4967,
      "longitude": 0.1764,
      "radius": 1.5,
      "radius_unit": "mi"
    }
  }
  ...
}
```

###### geo_named_area
This term matches a coordinate pair within the given named area.
The available names are split into 3 categories: `country`, `marine` and `geography`.
Each name in the list corresponds to a GeoJSON MultiPolygon which outlines the given area.
Holes are also supported for more complex areas.
The names available are available in the schema through the linked `geojson/v1.0.0-{}.json`
subschemas, where there is one for each category.

No `fields `are necessary as the `meta.geo` field is always used.


```json
{
  ...
  {
    "geo_named_area": {
      "country": "Curaçao"
    }
  }
  ...
}
```

###### geo_custom_area
This term matches a coordinate pair within the given GeoJSON MultiPolygon coordinates.
The array associated with the `geo_named_area` should be a valid GeoJSON multipolygon array.
This therefore allows for the use of multiple Polygons in one term (associated using an or query).
Additionally, holes in the Polygons are fully supported.
See the GeoJSON doc on Polygons and MultiPolygons to get familiar with them.

No `fields `are necessary as the `meta.geo` field is always used.

```json
{
  ...
  {
    "geo_custom_area": [[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]]]
  }
  ...
}
```

###### exists
This term matches records which don't have a value for the given field(s).
Because of the special nature of how geo queries are accomplished, to perform an exists check and
find out if a record has a geo component associated with it, leave the `fields` key out and include
`geo_field: true` instead.
At least one of `fields` or `geo_field` must be present.


```json
{
  ...
  {
    "exists": {
      "fields": [
        "family"
      ]
    }
  }
  ...
}
```


## Examples

### 1.json
```json
{}
```
This query is the most basic possible in `v1.0.0`.
It simply searches everything with no free text queries or filters.


### 2.json
```json
{
  "search": "mollusca"
}
```
This query searches all fields using the free text phrase "mollusca".


### 3.json
```json
{
  "filters": {
    "and": [
      {
        "string_equals": {
          "fields": [
            "genus"
          ],
          "value": "helix"
        }
      },
      {
        "string_contains": {
          "fields": [
            "higherGeography"
          ],
          "value": "europe"
        }
      }
    ]
  }
}
```
This query finds only records where the `genus` is `helix` and the `higherGeography` contains the
word `europe`.


### 4.json
```json
{
  "search": "italy",
  "filters": {
    "and": [
      {
        "string_equals": {
          "fields": [
            "genus"
          ],
          "value": "helix"
        }
      },
      {
        "string_contains": {
          "fields": [
            "higherGeography"
          ],
          "value": "europe"
        }
      }
    ]
  }
}
```
This query finds only records where the `genus` is `helix`, the `higherGeography` contains the
word `europe` and the word `italy` matches one of the fields in the record.
This query is analogous to:

```json
{
  "filters": {
    "and": [
      {
        "string_contains": {
          "fields": [],
          "value": "italy"
        }
      },
      {
        "string_equals": {
          "fields": [
            "genus"
          ],
          "value": "helix"
        }
      },
      {
        "string_contains": {
          "fields": [
            "higherGeography"
          ],
          "value": "europe"
        }
      }
    ]
  }
}
```


### 5.json
```json
{
  "filters": {
    "and": [
      {
        "string_equals": {
          "fields": [
            "genus"
          ],
          "value": "helix"
        }
      },
      {
        "or": [
          {
            "string_contains": {
              "fields": [
                "higherGeography"
              ],
              "value": "italy"
            }
          },
          {
            "string_contains": {
              "fields": [
                "higherGeography"
              ],
              "value": "spain"
            }
          },
          {
            "string_contains": {
              "fields": [
                "higherGeography"
              ],
              "value": "portugal"
            }
          }
        ]
      }
    ]
  }
}
```
This query finds records where the `genus` is `helix` and either the `higherGeography` contains
`italy`, `spain` or `portugal` (i.e. find all `helix` records from `italy`, `spain` or `portugal`).


### 6.json
```json
{
  "filters": {
    "and": [
      {
        "string_equals": {
          "fields": [
            "genus"
          ],
          "value": "helix"
        }
      },
      {
        "number_range": {
          "fields": [
            "year"
          ],
          "less_than": 2010,
          "less_than_inclusive": true,
          "greater_than": 2000,
          "greater_than_inclusive": true
        }
      },
      {
        "or": [
          {
            "string_contains": {
              "fields": [
                "higherGeography"
              ],
              "value": "italy"
            }
          },
          {
            "string_contains": {
              "fields": [
                "higherGeography"
              ],
              "value": "spain"
            }
          },
          {
            "string_contains": {
              "fields": [
                "higherGeography"
              ],
              "value": "portugal"
            }
          }
        ]
      }
    ]
  }
}
```
This query finds all records where `genus` is `helix`, `year` is between `2000` and `2010`, and have
a `higherGeography` value containing either `italy`, `spain` or `portugal` (i.e. find all `helix`
records from `italy`, `spain` or `portugal` from 2000 to 2010).


### 7.json
```json
{
  "filters": {
    "and": [
      {
        "string_equals": {
          "fields": [
            "genus"
          ],
          "value": "helix"
        }
      },
      {
        "geo_point": {
          "latitude": 51.4712,
          "longitude": -0.9421,
          "radius": 10,
          "radius_unit": "mi"
        }
      }
    ]
  }
}
```
This query finds all records where `genus` is `helix` and `meta.geo` is within `10 miles` of
`51.4712, -0.9421`.


### 8.json
```json
{
  "filters": {
    "and": [
      {
        "exists": {
          "fields": [
            "associatedMedia"
          ]
        }
      }
    ]
  }
}
```
This query finds all records which have the `associatedMedia` field present.


### 9.json
```json
{
  "filters": {
    "and": [
      {
        "exists": {
          "geo_field": true
        }
      }
    ]
  }
}
```
This query finds all records that have the `meta.geo` field present.


### 10.json
```json
{
  "filters": {
    "and": [
      {
        "geo_named_area": {
          "country": "Curaçao"
        }
      }
    ]
  }
}
```

This query finds all records where their `meta.geo` field resides inside the area defined as the
country of Curaçao.


### 11.json
```json
{
  "filters": {
    "and": [
      {
        "geo_custom_area": [
          [
            [
              [
                102.0,
                2.0
              ],
              [
                103.0,
                2.0
              ],
              [
                103.0,
                3.0
              ],
              [
                102.0,
                3.0
              ],
              [
                102.0,
                2.0
              ]
            ]
          ],
          [
            [
              [
                100.0,
                0.0
              ],
              [
                101.0,
                0.0
              ],
              [
                101.0,
                1.0
              ],
              [
                100.0,
                1.0
              ],
              [
                100.0,
                0.0
              ]
            ],
            [
              [
                100.2,
                0.2
              ],
              [
                100.8,
                0.2
              ],
              [
                100.8,
                0.8
              ],
              [
                100.2,
                0.8
              ],
              [
                100.2,
                0.2
              ]
            ]
          ]
        ]
      }
    ]
  }
}
```

This query finds all records where their `meta.geo` field resides inside the given GeoJSON
`MultiPolygon` coordinates.
The coordinates given correspond to two shapes, a square and a square with another square hole
within it (defining and area that the records cannot be in).


### 12.json
```json
{
  "filters": {
    "not": [
      {
        "string_equals": {
          "fields": [
            "genus"
          ],
          "value": "helix"
        }
      }
    ]
  }
}
```

This query finds only records where the `genus` _is not_ `helix`.
