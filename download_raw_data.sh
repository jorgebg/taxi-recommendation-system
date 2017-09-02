cat raw_data_urls_yellow_2016.txt | xargs -n 1 -P 6 wget -nc -c -P data/
wget -nc -c https://data.cityofnewyork.us/api/geospatial/tqmj-j8zm\?method\=export\&format\=GeoJSON -O nyc-boundaries.geojson
