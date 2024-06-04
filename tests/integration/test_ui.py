from discograph.app import app
from tests.integration.app_test_case import AppTestCase


class TestUI(AppTestCase):
    def setUp(self):
        self.app = app.test_client()
        app.debug = True

    def test_index(self):
        response = self.app.get("/")
        self.assertEqual("200 OK", response.status)

    def test_artist_200(self):
        response = self.app.get("/artist/2239")
        self.assertEqual("200 OK", response.status)

    def test_artist_400(self):
        response = self.app.get("/artist/bad")
        self.assertEqual("400 BAD REQUEST", response.status)

    def test_artist_404(self):
        response = self.app.get("/artist/0")
        self.assertEqual("404 NOT FOUND", response.status)

    def test_label_200(self):
        response = self.app.get("/label/1")
        self.assertEqual("200 OK", response.status)

    def test_label_400(self):
        response = self.app.get("/label/bad")
        self.assertEqual("400 BAD REQUEST", response.status)

    def test_label_404(self):
        response = self.app.get("/label/2")
        self.assertEqual("404 NOT FOUND", response.status)

    def test_error(self):
        response = self.app.get("/malformed")
        self.assertEqual("404 NOT FOUND", response.status)
