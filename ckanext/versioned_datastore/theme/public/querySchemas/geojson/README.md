# GeoJSON

## How to update the GeoJSON
This will download the [1:50m countries list from Natural Earth](https://www.naturalearthdata.com/downloads/50m-cultural-vectors/10m-admin-0-countries/) and then convert the data into GeoJSON format.
The `COORDINATE_PRECISION` parameter defines the number of decimal places to use in the GeoJSON and is set to 6.
According to [this excellent answer](https://gis.stackexchange.com/questions/8650/measuring-accuracy-of-latitude-and-longitude/8674#8674) that's about 0.11m precision, which is more than enough.

```bash
ogr2ogr -f GeoJSON -t_srs EPSG:4326 -lco COORDINATE_PRECISION=6 countries.geojson /vsizip/vsicurl/https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/50m/cultural/ne_10m_admin_0_countries.zip
```

Just swap in the URL you want to download and run!
