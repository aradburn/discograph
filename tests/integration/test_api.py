import json

from discograph.app import app
from tests.integration.app_test_case import AppTestCase


class TestAPI(AppTestCase):
    # @classmethod
    # def setUpClass(cls):
    #     super(TestAPI, cls).setUpClass()
    #
    # @classmethod
    # def tearDownClass(cls):
    #     super(TestAPI, cls).tearDownClass()

    def setUp(self):
        self.app = app.test_client()

    def test_network_01(self):
        response = self.app.get("/api/artist/network/2239")
        assert response.status == "200 OK"

    def test_network_02(self):
        response = self.app.get("/api/artist/network/999999999999")
        assert response.status == "404 NOT FOUND"

    def test_network_03(self):
        response = self.app.get("/api/label/network/1")
        assert response.status == "200 OK"

    def test_search_01(self):
        response = self.app.get("/api/search/Morris")
        assert response.status == "200 OK"
        actual = json.loads(response.data.decode("utf-8"))
        expected = {
            "results": [
                {"key": "artist-496270", "name": "Morris Gould"},
                {"key": "artist-33927", "name": "Stephen Morris"},
                {"key": "artist-3723", "name": "Chris Morris"},
                {"key": "artist-3985", "name": "Mixmaster Morris"},
                {"key": "artist-27005", "name": "Morris Nightingale"},
                {"key": "artist-444670", "name": "Craig Morris"},
                {"key": "artist-2922503", "name": "Paul Morris (17)"},
                {"key": "artist-175123", "name": 'Leo "Swift" Morris'},
                {"key": "artist-249982", "name": "Leo Swift Morris"},
            ]
        }
        self.maxDiff = None
        self.assertCountEqual(expected["results"], actual["results"])

    def test_search_02(self):
        response = self.app.get("/api/search/Chris+Morris")
        assert response.status == "200 OK"
        data = json.loads(response.data.decode("utf-8"))
        assert data == {"results": [{"key": "artist-3723", "name": "Chris Morris"}]}

    def test_relations_01(self):
        response = self.app.get("/api/artist/relations/32613")
        assert response.status == "200 OK"
