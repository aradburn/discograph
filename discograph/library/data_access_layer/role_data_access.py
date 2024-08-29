import logging
import re
from typing import Dict, List

from discograph.library.fields.role_type import RoleType

log = logging.getLogger(__name__)


class RoleDataAccess:
    # CLASS VARIABLES
    role_name_to_role_id_lookup: Dict[str, int] = {}
    role_id_to_role_category_lookup: Dict[int, RoleType.Category] = {}
    role_id_to_role_name_lookup: Dict[int, str] = {}

    @staticmethod
    def normalise_role_names(input_name: str) -> List[str]:
        # Split into multiple names if needed
        input_names = input_name.split(" & ")
        input_names_list = [name.split(" and ") for name in input_names]
        input_names = [name for name_list in input_names_list for name in name_list]
        input_names = [RoleDataAccess.normalise_role_name(name) for name in input_names]
        return input_names

    @staticmethod
    def normalise_role_name(input_name: str) -> str:
        def upper(match):
            return match.group(1).upper()

        def lower(match):
            return match.group(1).lower()

        def capitalize(match):
            return "(" + match.group(1).capitalize() + ")"

        if input_name.isupper():
            return input_name

        name = input_name
        name = re.sub(r"(-BY|-By|-by)($|\s)", " By", name)
        name = re.sub(r"(-TO|-To|-to)($|\s)", " To", name)
        name = re.sub(r"(-AT|-At|-at)($|\s)", " At", name)
        name = re.sub(r"(-WITH|-With|-with)($|\s)", " With", name)
        # name = re.sub(r"(\s|-)+(BY|By|by)($|\s)", "", name)
        # name = re.sub(r"(\s|-)+(TO|To|to)($|\s)", "", name)
        # name = re.sub(r"(\s|-)+(AT|At|at)($|\s)", "", name)
        # name = re.sub(r"(\s|-)+(FOR|For|for)($|\s)", "", name)
        # name = re.sub(r"(\s|-)+(ON|On|on)($|\s)", "", name)
        # name = re.sub(r"(\s|-)+(WITH|With|with)($|\s)", "", name)
        # name = name.replace(" By", "")
        # name = name.replace(" by", "")
        # name = name.replace("-By", "")
        # name = name.replace("-by", "")

        if name.lower() == "dj":
            return "DJ"
        if name.lower() == "cgi":
            return "CGI"
        if name.lower() == "dj mix":
            return "DJ Mix"
        if name.lower() == "cgi artist":
            return "CGI Artist"
        if name.lower() == "vibes":
            return "Vibraphone"
        if name.lower() == "remiz":
            return "Remix"

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
