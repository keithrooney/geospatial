import unittest
import mock

import mongomock

from geospatial import geospatial


class RadiusTest(unittest.TestCase):

    def test___eq__(self):
        radius0 = geospatial.Radius(6371000)

        self.assertTrue(radius0.__eq__(radius0))

        self.assertFalse(radius0.__eq__(None))
        self.assertFalse(radius0.__eq__(geospatial.Node()))

        radius1 = geospatial.Radius(6371000)

        self.assertTrue(radius0.__eq__(radius1))
        self.assertTrue(radius1.__eq__(radius0))

        radius2 = geospatial.Radius(3959)

        self.assertFalse(radius0.__eq__(radius2))
        self.assertFalse(radius2.__eq__(radius0))

    def test___hash__(self):

        radius = geospatial.Radius(6371000)

        expected = hash(6371000)
        actual = radius.__hash__()

        self.assertEquals(expected, actual)

    def test_convert_to(self):

        radius = geospatial.Radius(6371000)

        self.assertEquals(6371000, radius.convert_to(geospatial.Unit.METERS))
        self.assertEquals(6371, radius.convert_to(geospatial.Unit.KILOMETERS))
        self.assertEquals(3958.7558657440545, radius.convert_to(geospatial.Unit.MILES))
        self.assertRaises(ValueError, radius.convert_to, None)


class EarthsRadiusTest(unittest.TestCase):

    def test_earths_radius(self):
        self.assertEquals(6371000, geospatial.EarthsRadius.convert_to(geospatial.Unit.METERS))
        self.assertEquals(6371, geospatial.EarthsRadius.convert_to(geospatial.Unit.KILOMETERS))
        self.assertEquals(3958.7558657440545, geospatial.EarthsRadius.convert_to(geospatial.Unit.MILES))
        self.assertRaises(ValueError, geospatial.EarthsRadius.convert_to, None)


class GeospatialTest(unittest.TestCase):

    def test_haversine(self):
        self.assertEqual(0.0, geospatial.haversine((0.0, 0.0), (0.0, 0.0)))
        self.assertEqual(9385200.79836108, geospatial.haversine((35.188443, -157.813352), (-6.908650, 124.053931)))


class NodeTest(unittest.TestCase):

    def test___eq__(self):
        node0 = geospatial.Node(1, (35.0, -157.0), "node")

        self.assertTrue(node0.__eq__(node0))

        self.assertFalse(node0.__eq__(None))
        self.assertFalse(node0.__eq__(geospatial.Unit))

        node1 = geospatial.Node(2, (35.0, -157.0), "node")

        self.assertTrue(node0.__eq__(node1))
        self.assertTrue(node1.__eq__(node0))

    def test___hash__(self):
        node = geospatial.Node(1, (35.0, -157.0), "node")

        expected = hash("node")
        actual = node.__hash__()

        self.assertEquals(expected, actual)


class MongoNodeTest(unittest.TestCase):

    def test_mongo_node(self):
        node_id = geospatial.objectid.ObjectId(None)

        expected = geospatial.Node(
            node_id=node_id,
            coordinates=(0.0, 0.1),
            value="me"
        )

        actual = geospatial.MongoNode(**{
            "_id": node_id,
            "location": {
                "coordinates": [0.1, 0.0]
            },
            "value": "me"
        })

        self.assertEquals(expected, actual)


class MongoNodeIteratorTest(unittest.TestCase):

    def setUp(self):
        self.mock_cursor = mock.MagicMock()
        self.iterator = geospatial.MongoNodeIterator(self.mock_cursor)

    def test___iter__(self):
        self.assertEquals(self.iterator, iter(self.iterator))

    def test___next__(self):

        document = {
            "_id": geospatial.objectid.ObjectId(None),
            "location": {
                "coordinates": [0.1, 0.0]
            },
            "value": "me"
        }

        self.mock_cursor.next.return_value = document

        expected = geospatial.Node(
            node_id=str(document["_id"]),
            coordinates=(0.0, 0.1),
            value="me"
        )

        actual = self.iterator.__next__()

        self.assertEquals(expected, actual)


class InMemoryGeospatialRepositoryTest(unittest.TestCase):

    def setUp(self):
        self.repository = geospatial.InMemoryGeospatialRepository()

    def test_search(self):

        self.assertEqual(0, len(self.repository))

        me = geospatial.Node(coordinates=(54.098494, -6.242611), value="me")
        you = geospatial.Node(coordinates=(54.103859, -6.252195), value="you")
        him = geospatial.Node(coordinates=(54.035867, -6.307209), value="him")
        her = geospatial.Node(coordinates=(54.395999, -6.482304), value="her")
        them = geospatial.Node(coordinates=(54.387373, -7.017335), value="them")
        us = geospatial.Node(coordinates=(52.146571, -7.408515), value="us")

        for node in [me, you, him, her, them, us]:
            self.repository.upsert(node)

        self.assertEqual(6, len(self.repository))

        search_criteria = {
            geospatial.Radius(-100): [],
            geospatial.Radius(500): [me],
            geospatial.Radius(5000): [me, you],
            geospatial.Radius(10000): [me, you, him],
            geospatial.Radius(25000): [me, you, him],
            geospatial.Radius(50000): [me, you, him, her],
            geospatial.Radius(100000): [me, you, him, her, them],
            geospatial.Radius(250000): [me, you, him, her, them, us]
        }

        for radius, expectation in search_criteria.items():
            actual = self.repository.search((54.098494, -6.242611), radius)
            self.assertCountEqual(expectation, actual)

    def test_upsert(self):
        self.assertEqual(0, len(self.repository))

        node = geospatial.Node(coordinates=(54.098494, -6.242611), value="me")
        inserted_node = self.repository.upsert(node)

        self.assertTrue(self.repository.contains(inserted_node.node_id))

        node = geospatial.Node(inserted_node.node_id, (54.098494, -6.242611), "another")
        updated_node = self.repository.upsert(node)

        self.assertEquals(updated_node, self.repository.get(inserted_node.node_id))

    def test_delete_with_valid_node_id(self):
        node = self.repository.upsert(geospatial.Node(coordinates=(54.098494, -6.242611), value="me"))
        self.assertTrue(self.repository.delete(node.node_id))

    def test_delete_with_invalid_node_id(self):
        self.assertFalse(self.repository.delete(1232))


@mongomock.patch(servers=(('server.example.com', 27017),))
class MongoGeospatialRepositoryTest(unittest.TestCase):

    def setUp(self):
        self.repository = geospatial.MongoGeospatialRepository(mongomock.MongoClient("server.example.com", 27017))

    def test_search(self):

        node = geospatial.Node(
            node_id=geospatial.objectid.ObjectId(),
            coordinates=(0.0, 0.1),
            value="you"
        )

        nodes = iter([{
            "_id": node.node_id,
            "value": node.value,
            "location": {
                "coordinates": (node.coordinates[1], node.coordinates[0])
            }
        }])

        def next_node():
            return next(nodes)

        mock_collection = mock.MagicMock()
        mock_cursor = mock.MagicMock()

        self.repository.collection = mock_collection

        mock_collection.find.return_value = mock_cursor
        mock_cursor.next.side_effect = next_node

        iterable = self.repository.search((54.098494, -6.242611), geospatial.Radius(1000))

        self.assertEquals(node, next(iterable))
        self.assertRaises(StopIteration, iterable.__next__)

    def test_upsert(self):

        node = geospatial.Node(coordinates=(54.098494, -6.242611), value="me")
        inserted_node = self.repository.upsert(node)

        node = geospatial.Node(node_id=inserted_node.node_id, coordinates=(54.098494, -6.242611), value="other")
        updated_node = self.repository.upsert(node)

        self.assertEquals(updated_node, self.repository.get(updated_node.node_id))

    def test_delete_with_valid_node_id(self):
        node = self.repository.upsert(geospatial.Node(coordinates=(54.098494, -6.242611), value="me"))
        self.assertTrue(self.repository.delete(node.node_id))

    def test_delete_with_invalid_node_id(self):
        self.assertFalse(self.repository.delete(str(geospatial.objectid.ObjectId())))
