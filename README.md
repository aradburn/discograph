# Discograph2

Interactive visualization of the Discogs Database

## What is this?

Discograph2 visualizes the relationships between
musicians, bands and labels.

All of Discograph2's data is derived from the [Discogs](http://www.discogs.com)
discography database: nearly 7 million artists, 1 million labels, and 10 million
releases creating a network of over 100 million different relationships.

## How do I use this?

Visit the live site at https://discograph.azurewebsites.net.

Type an artist name into the search box and go!

What do all of these symbols mean?

* Small circles represent artists.
* Large circles represent bands.
* Solid lines show artist/band and sublabel/parent-label membership.
* Dashed lines show pseudonyms between artists (*AKA*).
* Dotted lines show all kinds of other relationships, like "artist X played
  guitar for artist Y" or "artist P did cover art for label Q".

The graph shows at most 100 entities at a time, so double-click on any circle
containing a plus-sign to reveal more connections.

As mentioned, the data used is straight from Discogs, so if there are any
omissions or errors, please add or correct the artist on their website.

# Development

The project uses `venv` to manage the Python environment and dependencies.

## Create a virtual environment

    python3 -m venv venv

## Activate the virtual environment

    source venv/bin/activate # On Unix or MacOS
    venv\Scripts\activate # On Windows

## Deactivate the virtual environment

    deactivate

## Package management

    pip install --upgrade pip

## Create requirements.txt

    pip freeze > requirements.txt

## Install packages from requirements.txt

    pip install -r requirements.txt



