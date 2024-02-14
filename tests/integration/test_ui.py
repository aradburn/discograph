from discograph.app import app
from tests.integration.app_test_case import AppTestCase


class TestUI(AppTestCase):
    # @classmethod
    # def setUpClass(cls):
    #     super(TestUI, cls).setUpClass()
    #
    # @classmethod
    # def tearDownClass(cls):
    #     super(TestUI, cls).tearDownClass()

    def setUp(self):
        self.app = app.test_client()

    def test_index(self):
        response = self.app.get("/")
        assert response.status == "200 OK"

    def test_artist_200(self):
        response = self.app.get("/artist/2239")
        assert response.status == "200 OK"

    def test_artist_400(self):
        response = self.app.get("/artist/bad")
        assert response.status == "400 BAD REQUEST"

    def test_artist_404(self):
        response = self.app.get("/artist/0")
        assert response.status == "404 NOT FOUND"

    def test_label_200(self):
        response = self.app.get("/label/1")
        assert response.status == "200 OK"

    def test_label_400(self):
        response = self.app.get("/label/bad")
        assert response.status == "400 BAD REQUEST"

    def test_label_404(self):
        response = self.app.get("/label/2")
        assert response.status == "404 NOT FOUND"

    def test_error(self):
        response = self.app.get("/malformed")
        assert response.status == "404 NOT FOUND"
