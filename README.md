# Geospatial

## Goal

To gain an understanding of the underlying math behind Geospatial locating, as well as, learning how to use MongoDB API. 

## Tests

All tests are contained in the [tests](./tests) directory.

## Example

Below is a quick example of the MongoGeospatialRepository, the InMemoryGeospatialRepository should work the same.

'''python
from geospatial.geospatial import MongoGeospatialRepository, Radius

repository = MongoGeospatialRepository()

me = Node(coordinates=(54.098494, -6.242611), value="me")
you = Node(coordinates=(54.103859, -6.252195), value="you")
him = Node(coordinates=(54.035867, -6.307209), value="him")
her = Node(coordinates=(54.395999, -6.482304), value="her")
them = Node(coordinates=(54.387373, -7.017335), value="them")
us = Node(coordinates=(52.146571, -7.408515), value="us")

for n in [you, him, her, them, us]:
    repository.upsert(n)

nodes = repository.search((54.098494, -6.242611), Radius(50000))

for node in nodes:
    print(node)

''' 