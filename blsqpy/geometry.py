from shapely.geometry import LineString, Polygon, MultiPolygon, Point, shape
import json

def as_geometry(coordinates):
    x = json.loads(coordinates)
    if coordinates.startswith("[[[["):
        geojson = {"type": "MultiPolygon", "coordinates": x}
        return shape(geojson)
    if coordinates.startswith("[[["):
        geojson = {"type": "Polygon", "coordinates": x}
        return shape(geojson)
    if coordinates.startswith("[["):
        return LineString(x)
    if coordinates.startswith("["):
        return Point(float(x[0]), float(x[1]))


def geometrify(orgunits):
    for orgunit in orgunits:
        if "coordinates" in orgunit:
            orgunit["geometry"] = as_geometry(orgunit["coordinates"])

