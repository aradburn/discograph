import logging
import re

log = logging.getLogger(__name__)


class RoleDataAccess:
    @staticmethod
    def normalize(input_name: str) -> str:
        def upper(match):
            return match.group(1).upper()

        def lower(match):
            return match.group(1).lower()

        def capitalize(match):
            return "(" + match.group(1).capitalize() + ")"

        if input_name.isupper():
            return input_name

        name = input_name.replace("-By", " By")
        name = name.replace("-by", " By")
        name = name.replace("Cgi", "CGI")
        name = name.replace("Dj", "DJ")

        if name == "Vibes":
            return "Vibraphone"
        if name == "Artwork":
            return "Artwork By"
        if name == "Artwork & Package Design By":
            return "Artwork By"

        name = " ".join(
            word.capitalize() if not word.isupper() else word
            for word in name.split(" ")
        )
        # Capitalise after hyphen
        name = re.sub(r"(-[a-z])", upper, name)
        # Do not capitalise before apostrophe
        name = re.sub(r"( [A-Z]')", lower, name)
        # Capitalise after apostrophe
        name = re.sub(r"('[a-z])", upper, name)
        # Capitalise after ampersand
        name = re.sub(r"(&[a-z])", upper, name)
        # Capitalise after round bracket
        name = re.sub(r"\(([a-z]{2,})\)", capitalize, name)
        # print(f"bracket: {bracket}, apos2: {apos2}")
        return name
