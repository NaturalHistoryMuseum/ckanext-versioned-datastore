# GeoJSON

This directory contains the raw data used to back the `geo_named_area` term as well as the schemas
for each category it uses.

## How to update the GeoJSON
**Note: if you want to update the GeoJSON you probably need to create a new query version!**

To run the following commands, you'll need to have `ogr2ogr` installed, it's usually found with the
various GDAL packages.

This will download the [1:50m countries list from Natural Earth](https://www.naturalearthdata.com/downloads/50m-cultural-vectors/10m-admin-0-countries/)
and then convert the data into GeoJSON format. The `COORDINATE_PRECISION` parameter defines the
number of decimal places to use in the GeoJSON and is set to 6. According to [this excellent answer](https://gis.stackexchange.com/questions/8650/measuring-accuracy-of-latitude-and-longitude/8674#8674)
that's about 0.11m precision, which is more than enough.

```bash
ogr2ogr -f GeoJSON -t_srs EPSG:4326 -lco COORDINATE_PRECISION=6 countries.geojson /vsizip/vsicurl/https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/50m/cultural/ne_10m_admin_0_countries.zip
```

Just swap in the URL you want to download and run!

## How to update the schemas
**Note: if you want to update the schemas you probably need to create a new query version!**

Here's a handy snippet to update the schemas:

```python
import json
import string

sets = [
    # the name keys here MUST match the ones used in the v1_0_0Schema class
    ('50m-admin-0-countries-v4.1.0.geojson', 'v1.0.0-countries.json', ('NAME_EN', 'NAME')),
    ('50m-geography-regions-v4.1.0.geojson', 'v1.0.0-geography.json', ('name_en', 'name')),
    ('50m-marine-regions-v4.1.0.geojson', 'v1.0.0-marine.json', ('name', )),
]

for source, schema, name_keys in sets:
    with open(source, 'r') as f:
        geojson = json.load(f)

    with open(schema, 'r') as f:
        schema = json.load(f)

    names = set()
    for feature in geojson['features']:
        # again, this MUST match the method used in the v1_0_0Schema class
        name = string.capwords(next(iter(
            filter(None, (feature[u'properties'].get(key, None) for key in name_keys)))))
        names.add(name)

    # replace the enum in the definitions, the next(iter()) is used so that we don't have to know
    # whether the key is marine/geography/country which is nice but also will break if the order of
    # the definitions in the schemas is changed, beware!
    next(iter(schema['definitions'].values()))['enum'] = sorted(names)

    with open(schema,  'w') as f:
        json.dump(schema, f, sort_keys=True, indent=2)
```

This script is designed to run under python3, if you need to run it under python2 make sure you
modify it to work correctly with UTF-8 values!
