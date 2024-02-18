from discograph import utils
from discograph.library.bootstrapper import Bootstrapper
from discograph.library.cockroach.cockroach_release import CockroachRelease
from tests.integration.library.cockroach.cockroach_test_case import CockroachTestCase


class TestCockroachRelease(CockroachTestCase):
    def test_01(self):
        iterator = Bootstrapper.get_iterator("release")
        release_element = next(iterator)
        release = CockroachRelease.from_element(release_element)
        actual = format(release)
        expected = utils.normalize(
            """
        {
            "artists": [
                {
                    "id": 41,
                    "name": "Autechre"
                }
            ],
            "companies": [],
            "country": "UK",
            "extra_artists": [
                {
                    "id": 445854,
                    "name": "Designers Republic, The",
                    "roles": [
                        {
                            "name": "Design"
                        }
                    ]
                },
                {
                    "anv": "Brown",
                    "id": 300407,
                    "name": "Rob Brown (3)",
                    "roles": [
                        {
                            "name": "Producer"
                        }
                    ]
                },
                {
                    "anv": "Booth",
                    "id": 42,
                    "name": "Sean Booth",
                    "roles": [
                        {
                            "name": "Producer"
                        }
                    ]
                }
            ],
            "formats": [
                {
                    "descriptions": [
                        "12\\"",
                        "EP",
                        "33 \\u2153 RPM",
                        "45 RPM"
                    ],
                    "name": "Vinyl",
                    "quantity": "1"
                }
            ],
            "genres": [
                "Electronic"
            ],
            "id": 157,
            "identifiers": [
                {
                    "description": null,
                    "type": "Barcode",
                    "value": "5 021603 054066"
                },
                {
                    "description": "Etching A",
                    "type": "Matrix / Runout",
                    "value": "WAP-54-A\\u2081 MA."
                },
                {
                    "description": "Etching B",
                    "type": "Matrix / Runout",
                    "value": "WAP-54-B\\u2081 MA."
                }
            ],
            "labels": [
                {
                    "catalog_number": "WAP54",
                    "name": "Warp Records"
                }
            ],
            "master_id": 1315,
            "notes": null,
            "random": null,
            "release_date": "1994-09-03 00:00:00",
            "styles": [
                "Abstract",
                "IDM",
                "Experimental"
            ],
            "title": "Anti EP",
            "tracklist": [
                {
                    "position": "A1",
                    "title": "Lost"
                },
                {
                    "position": "A2",
                    "title": "Djarum"
                },
                {
                    "position": "B",
                    "title": "Flutter"
                }
            ]
        }
        """
        )
        self.assertEqual(expected, actual)

    # noinspection PyUnusedLocal
    def test_02(self):
        iterator = Bootstrapper.get_iterator("release")
        release_element = next(iterator)
        release_element = next(iterator)
        release_element = next(iterator)
        release_element = next(iterator)
        release_element = next(iterator)
        release_element = next(iterator)
        release_element = next(iterator)
        release = CockroachRelease.from_element(release_element)
        actual = format(release)
        expected = utils.normalize(
            """
        {
            "artists": [
                {
                    "id": 939,
                    "name": "Higher Intelligence Agency, The"
                }
            ],
            "companies": [],
            "country": "UK",
            "extra_artists": [
                {
                    "id": 939,
                    "name": "Higher Intelligence Agency, The",
                    "roles": [
                        {
                            "name": "Written-By"
                        }
                    ]
                }
            ],
            "formats": [
                {
                    "descriptions": [
                        "EP"
                    ],
                    "name": "CD",
                    "quantity": "1"
                }
            ],
            "genres": [
                "Electronic"
            ],
            "id": 635,
            "identifiers": [
                {
                    "description": null,
                    "type": "Barcode",
                    "value": "5 018524 066308"
                },
                {
                    "description": null,
                    "type": "Matrix / Runout",
                    "value": "DISCTRONICS S HIA 2 CD 01"
                }
            ],
            "labels": [
                {
                    "catalog_number": "HIACD2",
                    "name": "Beyond"
                }
            ],
            "master_id": 21103,
            "notes": null,
            "random": null,
            "release_date": "1994-01-01 00:00:00",
            "styles": [
                "Techno",
                "Ambient"
            ],
            "title": "Colour Reform",
            "tracklist": [
                {
                    "duration": "8:49",
                    "extra_artists": [
                        {
                            "id": 932,
                            "name": "A Positive Life",
                            "roles": [
                                {
                                    "name": "Remix"
                                }
                            ]
                        }
                    ],
                    "position": "1",
                    "title": "Universal Entity (Ketamine Entity Reformed By A Positive Life)"
                },
                {
                    "duration": "6:24",
                    "extra_artists": [
                        {
                            "id": 41,
                            "name": "Autechre",
                            "roles": [
                                {
                                    "name": "Remix"
                                }
                            ]
                        }
                    ],
                    "position": "2",
                    "title": "Speech3 (Conoid Tone Reformed By Autechre)"
                },
                {
                    "duration": "8:30",
                    "extra_artists": [
                        {
                            "id": 379334,
                            "name": "Adrian Harrow",
                            "roles": [
                                {
                                    "name": "Engineer"
                                }
                            ]
                        },
                        {
                            "id": 953,
                            "name": "Irresistible Force, The",
                            "roles": [
                                {
                                    "name": "Remix"
                                }
                            ]
                        }
                    ],
                    "position": "3",
                    "title": "Speedlearn (Reformed By The Irresistible Force)"
                },
                {
                    "duration": "6:20",
                    "extra_artists": [
                        {
                            "id": 954,
                            "name": "Pentatonik",
                            "roles": [
                                {
                                    "name": "Remix"
                                }
                            ]
                        }
                    ],
                    "position": "4",
                    "title": "Alpha 1999 (Delta Reformed By Pentatonik)"
                }
            ]
        }
        """
        )
        self.assertEqual(expected, actual)
