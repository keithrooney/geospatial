import enum
import math

import pymongo
from bson import objectid


class Unit(enum.Enum):
    KILOMETERS = "KILOMETERS"
    METERS = "METERS"
    MILES = "MILES"


class Radius(object):

    def __init__(self, meters):
        self.meters = meters

    def __eq__(self, other):
        return isinstance(other, Radius) and self.meters == other.meters

    def __hash__(self):
        return hash(self.meters)

    def convert_to(self, unit):
        if unit == Unit.KILOMETERS:
            return self.to_kilometers()
        elif unit == Unit.MILES:
            return self.to_miles()
        elif unit == Unit.METERS:
            return self.to_meters()
        else:
            raise ValueError("Expected unit to be one of {0}, actual value is {1}.".format(Unit, unit))

    def to_meters(self):
        return self.meters

    def to_kilometers(self):
        return self.meters / 1000

    def to_miles(self):
        return self.meters / 1609.344


class EarthsRadius(enum.Enum):

    RADIUS = Radius(6371000)

    @staticmethod
    def convert_to(unit):
        return EarthsRadius.RADIUS.value.convert_to(unit)


def haversine(coordinates, other, unit=Unit.METERS):

    lat1, lon1 = coordinates
    lat2, lon2 = other

    phi_a, phi_b, delta_latitudes, delta_longitudes = map(math.radians, (lat1, lat2, (lat2 - lat1), (lon2 - lon1)))

    a = pow(math.sin(delta_latitudes / 2.0), 2) + math.cos(phi_a) * math.cos(phi_b) * pow(math.sin(delta_longitudes / 2.0), 2)
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))

    return EarthsRadius.convert_to(unit) * c


class Node(object):

    def __init__(self, node_id=None, coordinates=None, value=None):
        self.node_id = node_id
        self.coordinates = coordinates
        self.value = value

    def __hash__(self):
        return hash(self.value)

    def __eq__(self, other):
        return isinstance(other, Node) and self.value == other.value

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return self.__str__()


class MongoNode(Node):

    def __init__(self, _id, value, location):
        """
        MongoDB expects longitudes first, then latitudes when inserting, hence the reordering below.

        :param _id: the ObjectId of our document
        :param value: the value associated with this document.
        :param location: the location of the value stored as a Point.
        """
        coordinates = location["coordinates"]
        super(MongoNode, self).__init__(node_id=str(_id), coordinates=(coordinates[1], coordinates[0]), value=value)


class MongoNodeIterator(object):

    def __init__(self, cursor):
        self.cursor = cursor

    def __iter__(self):
        return self

    def __next__(self):
        return MongoNode(**self.cursor.next())


class GeospatialRepository(object):

    def search(self, coordinates, radius) -> list:
        return []

    def upsert(self, node) -> object:
        return None

    def get(self, node_id) -> object:
        return None

    def contains(self, node_id) -> bool:
        return False

    def delete(self, node_id) -> bool:
        return False


class InMemoryGeospatialRepository(GeospatialRepository):

    """
    A simple in memory geospatial cache to gain an understanding of how we might store values in a database, along with
    coordinates/location.
    """

    def __init__(self):
        self.nodes = {}

    def search(self, coordinates, radius):
        return [
            node for node_id, node in self.nodes.items()
            if haversine(coordinates, node.coordinates, Unit.METERS) <= radius.to_meters()
        ]

    def upsert(self, node):
        if node.node_id:
            node_id = node.node_id
        else:
            node_id = len(self.nodes) + 1
        n = Node(node_id=node_id, coordinates=node.coordinates, value=node.value)
        self.nodes[n.node_id] = n
        return n

    def get(self, node_id):
        if node_id in self.nodes:
            return self.nodes[node_id]
        else:
            return None

    def contains(self, node_id):
        return bool(self.get(node_id))

    def delete(self, node_id):
        try:
            return bool(self.nodes.pop(node_id))
        except KeyError:
            return False

    def __len__(self):
        return len(self.nodes)


class MongoGeospatialRepository(GeospatialRepository):

    def __init__(self, client=pymongo.MongoClient()):
        self.client = client
        self.database = self.client["geospatial"]
        self.collection = self.database["coordinates"]
        self.collection.create_index([("location", pymongo.GEOSPHERE)])

    def search(self, coordinates, radius):
        return MongoNodeIterator(self.collection.find({
            "location": {
                "$near": {
                    "$geometry": {
                        "type": "Point",
                        "coordinates": [coordinates[1], coordinates[0]]
                    },
                    "$maxDistance": int(radius.to_meters())
                }
            }
        }))

    def upsert(self, node):
        if node.node_id:
            node_id = node.node_id
        else:
            node_id = objectid.ObjectId(node.node_id)
        self.collection.replace_one({
            "_id": node_id
        }, {
            "_id": node_id,
            "value": node.value,
            "location": {
                "type": "Point",
                "coordinates": [node.coordinates[1], node.coordinates[0]]
            }
        }, upsert=True)
        return Node(node_id=node_id, coordinates=node.coordinates, value=node.value)

    def get(self, node_id) -> object:
        node = self.collection.find_one({"_id": objectid.ObjectId(node_id)})
        if node:
            return MongoNode(**node)
        else:
            return None

    def contains(self, node_id) -> bool:
        return bool(self.get(node_id))

    def delete(self, node_id):
        if node_id:
            result = self.collection.delete_one({"_id": objectid.ObjectId(node_id)})
            return result.deleted_count == 1
        else:
            raise KeyError("Expected node id not to be empty, actual value is.")

    def __len__(self):
        return self.collection.count()
