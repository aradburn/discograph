#! /usr/bin/env python

import setuptools


if __name__ == "__main__":
    setuptools.setup(
        author="Josiah Wolf Oberholtzer",
        author_email="josiah.oberholtzer@gmail.com",
        include_package_data=True,
        install_requires=[
            "Flask-Compress",
            "Flask-Mobility",
            "apsw",
            "flask",
            "gunicorn",
            "peewee",
            "psycopg2",
            "pytest",
            "redis",
        ],
        license="MIT",
        name="discograph",
        packages=[
            "discograph",
        ],
    )
