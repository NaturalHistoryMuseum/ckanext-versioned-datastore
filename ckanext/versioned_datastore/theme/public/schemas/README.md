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
`groups` wrap `terms` and other `groups` allowing the encapsulation of boolean logic (`and` and
`or`).
Within a group, there are 2 available `types`: `and` and `or`.

###### and
The `and` type ensures that **all** terms and groups in the `members` array of the group must be met
by the records being queried. Only records that meet all the constraints will be returned in the
results.

```json
{
    "type": "and",
    "members": [
      ...
    ]
}
```

###### or
The `or` ensures that **at least one** of the terms or groups in the `members` array of the group
are met by the records being queried. Only records that meet **at least one** of the constraints
will be returned in the results.

```json
{
    "type": "or",
    "members": [
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
    "type": "string_equals",
    "fields": [
      "genus"
    ],
    "value": "helix"
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
    "type": "string_contains",
    "fields": [
      "country"
    ],
    "value": "united kingdom"
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
    "type": "number_equals",
    "fields": [
      "length"
    ],
    "value": "45.19"
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
    "type": "number_range",
    "fields": [
      "year"
    ],
    "greater_than": 1974,
    "less_than": 2005,
    "less_than_inclusive": false
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
    "type": "geo_point",
    "latitude": 51.4967,
    "longitude": 0.1764,
    "radius": 1.5,
    "radius_unit": "mi"
  }
  ...
}
```

###### geo_named_area
This term matches a coordinate pair within the given named area.
The named areas available include country names and continents.
No `fields `are necessary as the `meta.geo` field is always used.


```json
{
  ...
  {
    "type": "geo_named_area",
    "name": "europe"
  }
  ...
}
```

###### geo_custom_area
This term matches a coordinate pair within the given GeoJSON area.
No `fields `are necessary as the `meta.geo` field is always used.

**TODO: the `geoJSON` value needs to be defined properly.**

```json
{
  ...
  {
    "type": "geo_named_area",
    "geoJSON": {
        "type": "Polygon",
          "coordinates": [[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]]]
        }
    }
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
    "type": "exists",
    "fields": [
      "family"
    ]
  }
  ...
}
```


## Examples

### 1.json
```json
{
  "query_version": "v1.0.0"
}
```
This query is the most basic possible in `v1.0.0`.
It simply searches everything with no free text queries or filters.


### 2.json
```json
{
  "query_version": "v1.0.0",
  "search": "mollusca"
}
```
This query searches all fields using the free text phrase "mollusca".


### 3.json
```json
{
  "query_version": "v1.0.0",
  "filters": {
    "type": "and",
    "members": [
      {
        "type": "string_equals",
        "fields": [
          "genus"
        ],
        "value": "helix"
      },
      {
        "type": "string_contains",
        "fields": [
          "higherGeography"
        ],
        "value": "europe"
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
  "query_version": "v1.0.0",
  "search": "italy",
  "filters": {
    "type": "and",
    "members": [
      {
        "type": "string_equals",
        "fields": [
          "genus"
        ],
        "value": "helix"
      },
      {
        "type": "string_contains",
        "fields": [
          "higherGeography"
        ],
        "value": "europe"
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
  "query_version": "v1.0.0",
  "filters": {
    "type": "and",
    "members": [
      {
        "type": "string_contains",
        "fields": [],
        "value": "italy"
      },
      {
        "type": "string_equals",
        "fields": [
          "genus"
        ],
        "value": "helix"
      },
      {
        "type": "string_contains",
        "fields": [
          "higherGeography"
        ],
        "value": "europe"
      }
    ]
  }
}
```


### 5.json
```json
{
  "query_version": "v1.0.0",
  "filters": {
    "type": "and",
    "members": [
      {
        "type": "string_equals",
        "fields": [
          "genus"
        ],
        "value": "helix"
      },
      {
        "type": "or",
        "members": [
          {
            "type": "string_contains",
            "fields": [
              "higherGeography"
            ],
            "value": "italy"
          },
          {
            "type": "string_contains",
            "fields": [
              "higherGeography"
            ],
            "value": "spain"
          },
          {
            "type": "string_contains",
            "fields": [
              "higherGeography"
            ],
            "value": "portugal"
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
  "query_version": "v1.0.0",
  "filters": {
    "type": "and",
    "members": [
      {
        "type": "string_equals",
        "fields": [
          "genus"
        ],
        "value": "helix"
      },
      {
        "type": "number_range",
        "fields": [
          "year"
        ],
        "less_than": 2010,
        "less_than_inclusive": true,
        "greater_than": 2000,
        "greater_than_inclusive": true
      },
      {
        "type": "or",
        "members": [
          {
            "type": "string_contains",
            "fields": [
              "higherGeography"
            ],
            "value": "italy"
          },
          {
            "type": "string_contains",
            "fields": [
              "higherGeography"
            ],
            "value": "spain"
          },
          {
            "type": "string_contains",
            "fields": [
              "higherGeography"
            ],
            "value": "portugal"
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
  "query_version": "v1.0.0",
  "filters": {
    "type": "and",
    "members": [
      {
        "type": "string_equals",
        "fields": [
          "genus"
        ],
        "value": "helix"
      },
      {
        "type": "geo_point",
        "latitude": 51.4712,
        "longitude": -0.9421,
        "radius": 10,
        "radius_unit": "mi"
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
  "query_version": "v1.0.0",
  "filters": {
    "type": "and",
    "members": [
      {
        "type": "exists",
        "fields": [
          "associatedMedia"
        ]
      }
    ]
  }
}
```
This query finds all records which have the `associatedMedia` field present.


### 9.json
```json
{
  "query_version": "v1.0.0",
  "filters": {
    "type": "and",
    "members": [
      {
        "type": "exists",
        "geo_field": true
      }
    ]
  }
}
```
This query finds all records that have the `meta.geo` field present.
