import json
import logging

from discograph import utils
from discograph.library import EntityType
from discograph.library.postgres.postgres_entity import PostgresEntity
from discograph.library.postgres.postgres_relation_grapher import (
    PostgresRelationGrapher,
)
from tests.integration.library.postgres.postgres_test_case import PostgresTestCase

log = logging.getLogger(__name__)


class TestPostgresRelationGrapher(PostgresTestCase):
    def setUp(self):
        super(TestPostgresRelationGrapher, self).setUp()

    """
    Problematic networks:

        - 296570: 306 nodes, 13688 links, 5 pages: 149, 4, 4, 4, 158
        - 1946151: unbalanced paging
        - 491160: bifurcated dense alias networks

    """

    json_kwargs = {
        "indent": 4,
        "separators": (",", ": "),
        "sort_keys": True,
    }

    def test___call___01(self):
        log.debug(PostgresEntity.database())
        artist = PostgresEntity.get(entity_type=EntityType.ARTIST, name="Seefeel")
        log.debug(f"artist: {artist}")
        roles = ["Alias", "Member Of"]
        grapher = PostgresRelationGrapher(
            artist,
            degree=1,
            roles=roles,
        )
        network = grapher.__call__()
        log.debug(f"network: {network}")
        actual = json.dumps(network, **self.json_kwargs)
        expected = utils.normalize(
            """
            {
                "center": {                
                    "key": "artist-2239",                
                    "name": "Seefeel"                
                },                
                "links": [                
                    {                
                        "key": "artist-115880-member-of-artist-2239",                
                        "pages": [                
                            1                
                        ],                
                        "role": "Member Of",                
                        "source": "artist-115880",                
                        "target": "artist-2239"                
                    },                
                    {                
                        "key": "artist-41103-member-of-artist-2239",                
                        "pages": [                
                            1                
                        ],                
                        "role": "Member Of",                
                        "source": "artist-41103",                
                        "target": "artist-2239"                
                    },                
                    {                
                        "key": "artist-489350-member-of-artist-2239",                
                        "pages": [                
                            1                
                        ],                
                        "role": "Member Of",                
                        "source": "artist-489350",                
                        "target": "artist-2239"                
                    },                
                    {                
                        "key": "artist-51674-member-of-artist-2239",                
                        "pages": [                
                            1                
                        ],                
                        "role": "Member Of",                
                        "source": "artist-51674",                
                        "target": "artist-2239"                
                    },                
                    {                
                        "key": "artist-66803-member-of-artist-2239",                
                        "pages": [                
                            1                
                        ],                
                        "role": "Member Of",                
                        "source": "artist-66803",                
                        "target": "artist-2239"                
                    }                
                ],                
                "nodes": [                
                    {                
                        "distance": 0,                
                        "id": 2239,                
                        "key": "artist-2239",                
                        "links": [                
                            "artist-115880-member-of-artist-2239",                
                            "artist-41103-member-of-artist-2239",                
                            "artist-489350-member-of-artist-2239",                
                            "artist-51674-member-of-artist-2239",                
                            "artist-66803-member-of-artist-2239"                
                        ],                
                        "missing": 0,                
                        "name": "Seefeel",                
                        "pages": [                
                            1                
                        ],                
                        "size": 5,                
                        "type": "artist"                
                    },                
                    {                
                        "cluster": 2,                
                        "distance": 1,                
                        "id": 41103,                
                        "key": "artist-41103",                
                        "links": [                
                            "artist-41103-member-of-artist-2239"                
                        ],                
                        "missing": 1,                
                        "name": "Mark Van Hoen",                
                        "pages": [                
                            1                
                        ],                
                        "size": 0,                
                        "type": "artist"                
                    },                
                    {                
                        "cluster": 1,                
                        "distance": 1,                
                        "id": 51674,                
                        "key": "artist-51674",                
                        "links": [                
                            "artist-51674-member-of-artist-2239"                
                        ],                
                        "missing": 3,                
                        "name": "Mark Clifford",                
                        "pages": [                
                            1                
                        ],                
                        "size": 0,                
                        "type": "artist"                
                    },                
                    {                
                        "distance": 1,                
                        "id": 66803,                
                        "key": "artist-66803",                
                        "links": [                
                            "artist-66803-member-of-artist-2239"                
                        ],                
                        "missing": 0,                
                        "name": "Daren Seymour",                
                        "pages": [                
                            1                
                        ],                
                        "size": 0,                
                        "type": "artist"                
                    },                
                    {                
                        "distance": 1,                
                        "id": 115880,                
                        "key": "artist-115880",                
                        "links": [                
                            "artist-115880-member-of-artist-2239"                
                        ],                
                        "missing": 0,                
                        "name": "Sarah Peacock",                
                        "pages": [                
                            1                
                        ],                
                        "size": 0,                
                        "type": "artist"                
                    },                
                    {                
                        "distance": 1,                
                        "id": 489350,                
                        "key": "artist-489350",                
                        "links": [                
                            "artist-489350-member-of-artist-2239"                
                        ],                
                        "missing": 0,                
                        "name": "Justin Fletcher",                
                        "pages": [                
                            1                
                        ],                
                        "size": 0,                
                        "type": "artist"                
                    }                
                ],                
                "pages": 1                
            }
        """
        )
        assert actual == expected

    def test___call___02(self):
        artist = PostgresEntity.get(
            entity_type=EntityType.ARTIST, name="Justin Fletcher"
        )
        roles = ["Alias", "Member Of"]
        grapher = PostgresRelationGrapher(
            artist,
            degree=2,
            max_nodes=5,
            roles=roles,
        )
        network = grapher.__call__()
        actual = json.dumps(network, **self.json_kwargs)
        expected = utils.normalize(
            """
            {            
                "center": {                
                    "key": "artist-489350",                
                    "name": "Justin Fletcher"                
                },                
                "links": [                
                    {                
                        "key": "artist-115880-member-of-artist-2239",                
                        "pages": [                
                            1                
                        ],                
                        "role": "Member Of",                
                        "source": "artist-115880",                
                        "target": "artist-2239"                
                    },                
                    {                
                        "key": "artist-41103-member-of-artist-2239",                
                        "pages": [                
                            1                
                        ],                
                        "role": "Member Of",                
                        "source": "artist-41103",                
                        "target": "artist-2239"                
                    },                
                    {                
                        "key": "artist-489350-member-of-artist-2239",                
                        "pages": [                
                            1              
                        ],                
                        "role": "Member Of",                
                        "source": "artist-489350",                
                        "target": "artist-2239"                
                    },                
                    {                
                        "key": "artist-51674-member-of-artist-2239",                
                        "pages": [                
                            1                
                        ],                
                        "role": "Member Of",                
                        "source": "artist-51674",                
                        "target": "artist-2239"                
                    },                
                    {                
                        "key": "artist-66803-member-of-artist-2239",                
                        "pages": [                
                            1                
                        ],                
                        "role": "Member Of",                
                        "source": "artist-66803",                
                        "target": "artist-2239"                
                    }                
                ],                
                "nodes": [                
                    {                
                        "distance": 1,                
                        "id": 2239,                
                        "key": "artist-2239",                
                        "links": [                
                            "artist-115880-member-of-artist-2239",                
                            "artist-41103-member-of-artist-2239",                
                            "artist-489350-member-of-artist-2239",                
                            "artist-51674-member-of-artist-2239",                
                            "artist-66803-member-of-artist-2239"                
                        ],                
                        "missing": 0,           
                        "name": "Seefeel",                
                        "pages": [                
                            1              
                        ],                
                        "size": 5,                
                        "type": "artist"                
                    },                
                    {                
                        "cluster": 2,                
                        "distance": 2,                
                        "id": 41103,                
                        "key": "artist-41103",                
                        "links": [                
                            "artist-41103-member-of-artist-2239"                
                        ],                
                        "missing": 1,                
                        "name": "Mark Van Hoen",                
                        "pages": [                
                            1                
                        ],                
                        "size": 0,                
                        "type": "artist"                
                    },                
                    {                
                        "cluster": 1,                
                        "distance": 2,                
                        "id": 51674,                
                        "key": "artist-51674",                
                        "links": [                
                            "artist-51674-member-of-artist-2239"                
                        ],                
                        "missing": 3,                
                        "name": "Mark Clifford",                
                        "pages": [                
                            1                
                        ],                
                        "size": 0,                
                        "type": "artist"                
                    },                
                    {                
                        "distance": 2,                
                        "id": 66803,                
                        "key": "artist-66803",                
                        "links": [                
                            "artist-66803-member-of-artist-2239"                
                        ],                
                        "missing": 0,                
                        "name": "Daren Seymour",                
                        "pages": [                
                            1                
                        ],                
                        "size": 0,                
                        "type": "artist"                
                    },                
                    {                
                        "distance": 2,                
                        "id": 115880,                
                        "key": "artist-115880",                
                        "links": [                
                            "artist-115880-member-of-artist-2239"                
                        ],                
                        "missing": 0,                
                        "name": "Sarah Peacock",                
                        "pages": [                
                            1                
                        ],                
                        "size": 0,                
                        "type": "artist"                
                    },                
                    {                
                        "distance": 0,                
                        "id": 489350,                
                        "key": "artist-489350",                
                        "links": [                
                            "artist-489350-member-of-artist-2239"                
                        ],                
                        "missing": 0,                
                        "name": "Justin Fletcher",                
                        "pages": [                
                            1              
                        ],                
                        "size": 0,                
                        "type": "artist"                
                    }                
                ],                
                "pages": 1                
            }
        """
        )
        assert actual == expected

    def test___call___03(self):
        artist = PostgresEntity.get(
            entity_type=EntityType.ARTIST, name="Justin Fletcher"
        )
        roles = ["Alias", "Member Of"]
        grapher = PostgresRelationGrapher(
            artist,
            degree=2,
            link_ratio=2,
            roles=roles,
        )
        network = grapher.__call__()
        actual = json.dumps(network, **self.json_kwargs)
        expected = utils.normalize(
            """
            {
                "center": {
                    "key": "artist-489350",
                    "name": "Justin Fletcher"
                },
                "links": [
                    {
                        "key": "artist-115880-member-of-artist-2239",
                        "pages": [
                            1
                        ],
                        "role": "Member Of",
                        "source": "artist-115880",
                        "target": "artist-2239"
                    },
                    {
                        "key": "artist-41103-member-of-artist-2239",
                        "pages": [
                            1
                        ],
                        "role": "Member Of",
                        "source": "artist-41103",
                        "target": "artist-2239"
                    },
                    {
                        "key": "artist-489350-member-of-artist-2239",
                        "pages": [
                            1
                        ],
                        "role": "Member Of",
                        "source": "artist-489350",
                        "target": "artist-2239"
                    },
                    {
                        "key": "artist-51674-member-of-artist-2239",
                        "pages": [
                            1
                        ],
                        "role": "Member Of",
                        "source": "artist-51674",
                        "target": "artist-2239"
                    },
                    {
                        "key": "artist-66803-member-of-artist-2239",
                        "pages": [
                            1
                        ],
                        "role": "Member Of",
                        "source": "artist-66803",
                        "target": "artist-2239"
                    }
                ],
                "nodes": [
                    {
                        "distance": 1,
                        "id": 2239,
                        "key": "artist-2239",
                        "links": [
                            "artist-115880-member-of-artist-2239",
                            "artist-41103-member-of-artist-2239",
                            "artist-489350-member-of-artist-2239",
                            "artist-51674-member-of-artist-2239",
                            "artist-66803-member-of-artist-2239"
                        ],
                        "missing": 0,
                        "name": "Seefeel",
                        "pages": [
                            1
                        ],
                        "size": 5,
                        "type": "artist"
                    },
                    {
                        "cluster": 2,
                        "distance": 2,
                        "id": 41103,
                        "key": "artist-41103",
                        "links": [
                            "artist-41103-member-of-artist-2239"
                        ],
                        "missing": 1,
                        "name": "Mark Van Hoen",
                        "pages": [
                            1
                        ],
                        "size": 0,
                        "type": "artist"
                    },
                    {
                        "cluster": 1,
                        "distance": 2,
                        "id": 51674,
                        "key": "artist-51674",
                        "links": [
                            "artist-51674-member-of-artist-2239"
                        ],
                        "missing": 3,
                        "name": "Mark Clifford",
                        "pages": [
                            1
                        ],
                        "size": 0,
                        "type": "artist"
                    },
                    {
                        "distance": 2,
                        "id": 66803,
                        "key": "artist-66803",
                        "links": [
                            "artist-66803-member-of-artist-2239"
                        ],
                        "missing": 0,
                        "name": "Daren Seymour",
                        "pages": [
                            1
                        ],
                        "size": 0,
                        "type": "artist"
                    },
                    {
                        "distance": 2,
                        "id": 115880,
                        "key": "artist-115880",
                        "links": [
                            "artist-115880-member-of-artist-2239"
                        ],
                        "missing": 0,
                        "name": "Sarah Peacock",
                        "pages": [
                            1
                        ],
                        "size": 0,
                        "type": "artist"
                    },
                    {
                        "distance": 0,
                        "id": 489350,
                        "key": "artist-489350",
                        "links": [
                            "artist-489350-member-of-artist-2239"
                        ],
                        "missing": 0,
                        "name": "Justin Fletcher",
                        "pages": [
                            1
                        ],
                        "size": 0,
                        "type": "artist"
                    }
                ],
                "pages": 1
            }
        """
        )
        assert actual == expected

    def test___call___04(self):
        """
        Missing count takes into account structural roles: members,
        aliases, groups, sublabels, parent labels, etc.
        """
        artist = PostgresEntity.get(
            entity_type=EntityType.ARTIST,
            entity_id=489350,
        )
        roles = ["Alias", "Member Of"]
        grapher = PostgresRelationGrapher(
            artist,
            degree=12,
            roles=roles,
        )
        network = grapher.__call__()
        actual = json.dumps(network, **self.json_kwargs)
        log.debug(f"network: {actual}")
        expected = utils.normalize(
            """
            {
                "center": {
                    "key": "artist-489350",
                    "name": "Justin Fletcher"
                },
                "links": [
                    {
                        "key": "artist-115880-member-of-artist-2239",
                        "pages": [
                            1
                        ],
                        "role": "Member Of",
                        "source": "artist-115880",
                        "target": "artist-2239"
                    },
                    {
                        "key": "artist-1920-alias-artist-51674",
                        "pages": [
                            1
                        ],
                        "role": "Alias",
                        "source": "artist-1920",
                        "target": "artist-51674"
                    },
                    {
                        "key": "artist-231-alias-artist-1920",
                        "pages": [
                            1
                        ],
                        "role": "Alias",
                        "source": "artist-231",
                        "target": "artist-1920"
                    },
                    {
                        "key": "artist-231-alias-artist-51674",
                        "pages": [
                            1
                        ],
                        "role": "Alias",
                        "source": "artist-231",
                        "target": "artist-51674"
                    },
                    {
                        "key": "artist-3490-alias-artist-41103",
                        "pages": [
                            1
                        ],
                        "role": "Alias",
                        "source": "artist-3490",
                        "target": "artist-41103"
                    },
                    {
                        "key": "artist-41103-member-of-artist-2239",
                        "pages": [
                            1
                        ],
                        "role": "Member Of",
                        "source": "artist-41103",
                        "target": "artist-2239"
                    },
                    {
                        "key": "artist-489350-member-of-artist-2239",
                        "pages": [
                            1
                        ],
                        "role": "Member Of",
                        "source": "artist-489350",
                        "target": "artist-2239"
                    },
                    {
                        "key": "artist-51674-member-of-artist-1656080",
                        "pages": [
                            1
                        ],
                        "role": "Member Of",
                        "source": "artist-51674",
                        "target": "artist-1656080"
                    },
                    {
                        "key": "artist-51674-member-of-artist-2239",
                        "pages": [
                            1
                        ],
                        "role": "Member Of",
                        "source": "artist-51674",
                        "target": "artist-2239"
                    },
                    {
                        "key": "artist-66803-member-of-artist-2239",
                        "pages": [
                            1
                        ],
                        "role": "Member Of",
                        "source": "artist-66803",
                        "target": "artist-2239"
                    }
                ],
                "nodes": [
                    {
                        "cluster": 1,
                        "distance": 3,
                        "id": 231,
                        "key": "artist-231",
                        "links": [
                            "artist-231-alias-artist-1920",
                            "artist-231-alias-artist-51674"
                        ],
                        "missing": 0,
                        "name": "Woodenspoon",
                        "pages": [
                            1
                        ],
                        "size": 0,
                        "type": "artist"
                    },
                    {
                        "cluster": 1,
                        "distance": 3,
                        "id": 1920,
                        "key": "artist-1920",
                        "links": [
                            "artist-1920-alias-artist-51674",
                            "artist-231-alias-artist-1920"
                        ],
                        "missing": 0,
                        "name": "Disjecta",
                        "pages": [
                            1
                        ],
                        "size": 0,
                        "type": "artist"
                    },
                    {
                        "distance": 1,
                        "id": 2239,
                        "key": "artist-2239",
                        "links": [
                            "artist-115880-member-of-artist-2239",
                            "artist-41103-member-of-artist-2239",
                            "artist-489350-member-of-artist-2239",
                            "artist-51674-member-of-artist-2239",
                            "artist-66803-member-of-artist-2239"
                        ],
                        "missing": 0,
                        "name": "Seefeel",
                        "pages": [
                            1
                        ],
                        "size": 5,
                        "type": "artist"
                    },
                    {
                        "cluster": 2,
                        "distance": 3,
                        "id": 3490,
                        "key": "artist-3490",
                        "links": [
                            "artist-3490-alias-artist-41103"
                        ],
                        "missing": 0,
                        "name": "Locust",
                        "pages": [
                            1
                        ],
                        "size": 0,
                        "type": "artist"
                    },
                    {
                        "cluster": 2,
                        "distance": 2,
                        "id": 41103,
                        "key": "artist-41103",
                        "links": [
                            "artist-3490-alias-artist-41103",
                            "artist-41103-member-of-artist-2239"
                        ],
                        "missing": 0,
                        "name": "Mark Van Hoen",
                        "pages": [
                            1
                        ],
                        "size": 0,
                        "type": "artist"
                    },
                    {
                        "cluster": 1,
                        "distance": 2,
                        "id": 51674,
                        "key": "artist-51674",
                        "links": [
                            "artist-1920-alias-artist-51674",
                            "artist-231-alias-artist-51674",
                            "artist-51674-member-of-artist-1656080",
                            "artist-51674-member-of-artist-2239"
                        ],
                        "missing": 0,
                        "name": "Mark Clifford",
                        "pages": [
                            1
                        ],
                        "size": 0,
                        "type": "artist"
                    },
                    {
                        "distance": 2,
                        "id": 66803,
                        "key": "artist-66803",
                        "links": [
                            "artist-66803-member-of-artist-2239"
                        ],
                        "missing": 0,
                        "name": "Daren Seymour",
                        "pages": [
                            1
                        ],
                        "size": 0,
                        "type": "artist"
                    },
                    {
                        "distance": 2,
                        "id": 115880,
                        "key": "artist-115880",
                        "links": [
                            "artist-115880-member-of-artist-2239"
                        ],
                        "missing": 0,
                        "name": "Sarah Peacock",
                        "pages": [
                            1
                        ],
                        "size": 0,
                        "type": "artist"
                    },
                    {
                        "distance": 0,
                        "id": 489350,
                        "key": "artist-489350",
                        "links": [
                            "artist-489350-member-of-artist-2239"
                        ],
                        "missing": 0,
                        "name": "Justin Fletcher",
                        "pages": [
                            1
                        ],
                        "size": 0,
                        "type": "artist"
                    },
                    {
                        "distance": 3,
                        "id": 1656080,
                        "key": "artist-1656080",
                        "links": [
                            "artist-51674-member-of-artist-1656080"
                        ],
                        "missing": 0,
                        "name": "Cliffordandcalix",
                        "pages": [
                            1
                        ],
                        "size": 1,
                        "type": "artist"
                    }
                ],
                "pages": 1
            }
        """
        )
        assert actual == expected

    def test___call___05(self):
        artist = PostgresEntity.get(
            entity_type=EntityType.LABEL,
            name="Lab Studio, Berlin",
        )
        roles = ["Recorded At"]
        grapher = PostgresRelationGrapher(
            artist,
            degree=2,
            roles=roles,
        )
        network = grapher.__call__()  # Should not error.
        assert network is not None
