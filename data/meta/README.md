A file `stations.geojson` is required. An example file is provided. 

The example file was downloaded from OSM by using the [OverPass](https://overpass-turbo.eu/). 

Query:
```
[out:json];
(
  node["railway"="station"](51.28,-0.51,51.686,0.238);
);
out body;
>;
out skel qt;
```