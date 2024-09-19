import functools
import logging
import re
from collections import deque
from typing import Dict, List, Set

from rapidfuzz import process, fuzz

from discograph.library.fields.role_type import RoleType

log = logging.getLogger(__name__)


class RoleDataAccess:

    # CLASS VARIABLES
    role_name_to_role_id_lookup: Dict[str, int] = {}
    role_name_set: Set[str] = set()
    role_id_to_role_category_lookup: Dict[int, RoleType.Category] = {}
    role_id_to_role_name_lookup: Dict[int, str] = {}
    # role_word_lookup: Set[str] = set()

    # REGEXs
    SPLIT_CHARACTERS = re.compile(r" & |&|＆| and | And |/|; |\+| - |・| Und | Et ")
    A_N_R = re.compile(r"\b[aA] ?(&|and|And|\+) ?[rR]\b")
    BRACKETS = re.compile(r"[({\[［].*[)}\]］]")
    DIGITS_AND_SPECIAL_CHARACTERS = re.compile(r"^\d+[-.)]*$|^[-*+.?/!`—•]+$")

    @staticmethod
    def normalise_role_names(input_name: str) -> List[str]:
        # original_name = input_name

        # Remove anything in brackets
        input_name = re.sub(RoleDataAccess.BRACKETS, "", input_name)
        input_name = re.sub(r"\".*\"| '.*' | '.*'$|^'.*'$|\([^)]*$", "", input_name)
        # input_name = re.sub(r"\".*\"", "", input_name)
        # input_name = re.sub(r" '.*' ", "", input_name)
        # input_name = re.sub(r" '.*'$", "", input_name)
        # input_name = re.sub(r"^'.*'$", "", input_name)
        # input_name = re.sub(r"\([^)]*$", "", input_name)

        # A&R change to avoid splitting
        input_name = re.sub(RoleDataAccess.A_N_R, "ANR", input_name)

        # Split into multiple names if needed
        input_names = re.split(RoleDataAccess.SPLIT_CHARACTERS, input_name)
        # print(f"    {input_names}")

        input_names = [
            RoleDataAccess.normalise_role_name(name)
            for name in input_names
            if name is not None and name != ""
        ]
        input_names_list = [
            re.split(r"\. ", name)
            for name in input_names
            if name is not None and name != ""
        ]
        input_names = [name for name_list in input_names_list for name in name_list]
        input_names = [
            RoleDataAccess.normalise_role_name(name)
            for name in input_names
            if name is not None and name != ""
        ]
        # log.debug(f"  {original_name} -> {input_names}")
        return input_names

    @staticmethod
    @functools.lru_cache(maxsize=100000)
    def normalise_role_name(input_name: str) -> str:
        def upper(matches):
            return matches.group(1).upper()

        def to_upper(matches):
            return matches.group(1) + matches.group(2).upper()

        def to_lower(matches):
            return matches.group(1) + matches.group(2).lower()

        def to_lower_upper(matches):
            return matches.group(1).lower() + matches.group(2).upper()

        def lower(matches):
            return matches.group(1).lower()

        def capitalize(matches):
            return "(" + matches.group(1).capitalize() + ")"

        if (
            re.match(r"^[A-Z]+$", input_name)
            and not input_name == "ANR"
            and not input_name == "FX"
        ):
            # print("Is all uppercase")
            return input_name

        name = input_name

        # Remove anything in brackets
        name = re.sub(RoleDataAccess.BRACKETS, "", name)

        name = re.sub(r"-BY\b", " By", name, flags=re.IGNORECASE)
        name = re.sub(r"-TO\b", " To", name, flags=re.IGNORECASE)
        name = re.sub(r"-AT\b", " At", name, flags=re.IGNORECASE)
        name = re.sub(r"-WITH\b", " With", name, flags=re.IGNORECASE)
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

        # Numbers that are instruments
        name_lower = name.lower()
        if name == "303" or name_lower == "tb-303":
            return "Roland TB-303"
        if name == "606" or name_lower == "tr-606":
            return "Roland TR-606"
        if name == "707" or name_lower == "tr-707":
            return "Roland TR-707"
        if name == "808" or name_lower == "tr-808":
            return "Roland TR-808"
        if name == "909" or name_lower == "tr-909":
            return "Roland TR-909"

        name = " ".join(
            word.capitalize() if not word.isupper() else word
            for word in name.split(" ")
        )
        # Capitalise after hyphen
        name = re.sub(r"(-[a-z])", upper, name)
        # print(f"{name}")
        # Do not capitalise before apostrophe
        # name = re.sub(r"( [A-Z]')", lower, name)
        # Capitalise after apostrophe if first letter
        name = re.sub(r"(\A| )('[a-zA-Z])", to_upper, name)
        name = re.sub(r"(\w)('[a-zA-Z])", to_lower, name)
        name = re.sub(r"( [dD])('[a-zA-Z])", to_lower_upper, name)
        # name = re.sub(r"[\b ]('[a-zA-Z])", r"\U\1", name)
        # Capitalise after apostrophe
        # name = re.sub(r"('[a-z])", upper, name)
        # Capitalise after ampersand
        # name = re.sub(r"(&[a-z])", upper, name)
        # Capitalise after round bracket
        # name = re.sub(r"\(([a-z]{2,})\)", capitalize, name)
        # print(f"bracket: {bracket}, apos2: {apos2}")

        # Single quote
        # name = re.sub(r" '", " ", name)

        # Underscore
        name = name.replace("_", " ")

        # Abbreviations
        name = re.sub(r"\bDJ\b", "DJ", name, flags=re.IGNORECASE)
        name = re.sub(r"\bVJ\b", "VJ", name, flags=re.IGNORECASE)
        name = re.sub(r"\bCGI\b", "CGI", name, flags=re.IGNORECASE)

        # Spellings
        name = re.sub(r"\bCo- ", "Co-", name)

        # Char + Digits
        name = re.sub(r"^[A-Z]\d+$", "", name)

        # Track
        name = re.sub(r"^Track(s)*\sA*\d*-*\d+", "", name)

        # Ignore with only digits or special characters
        if re.match(RoleDataAccess.DIGITS_AND_SPECIAL_CHARACTERS, name):
            # print("  Ignore with only digits or special characters")
            return ""
        if match := re.search(r"^\d+[^\d-]{1} +(.*)$", name):
            # print("  Remove digit + char")
            name = match.group(1)

        # Convert guitar strings and numbers
        name = re.sub(r"[- ]+Sting", "-String", name)
        name = re.sub(r"[- ]+Str ", "-String", name)
        name = re.sub(r"[- ]+Str$", "-String Guitar", name)
        name = re.sub(r"[- ]+Str$", "-String Guitar", name)
        name = re.sub(r"[- ]+String-$", "-String Guitar", name)
        name = re.sub(r"[- ]+Stringed-", "-Stringed ", name)
        if not re.search(r"Roland|Solina|Arp", name, flags=re.IGNORECASE):
            name = re.sub(r"[- ]+String ", "-String ", name)
        name = re.sub(r"[sS]tring-Guitar", "String Guitar", name)
        name = re.sub(r"^(.*-String )*Acoustic$", r"\1Acoustic Guitar", name)
        # name = re.sub(r"^(.*-String |Electric )*Bass$", r"\1Bass Guitar", name)
        name = re.sub(r"-Strings", "-String", name)
        name = re.sub(r"\bGuitars\b", "Guitar", name)
        name = re.sub(r"^Acoustic$", "Acoustic Guitar", name)

        # Strings
        if match := re.search(r"^(\d+)( *)[sS]tring$", name):
            # print("  Add hyphen")
            name = match.group(1) + "-String Guitar"
        if match := re.search(r"^(\d+)( *)[sS]tring Guitar$", name):
            # print("  Add hyphen")
            name = match.group(1) + "-String Guitar"

        # Convert number of strings
        if re.search(r"String", name):
            name = re.sub(r"20[- ]String", "Twenty-String", name)
            name = re.sub(r"10[- ]String", "Ten-String", name)
            name = re.sub(r"11[- ]String", "Eleven-String", name)
            name = re.sub(r"12[- ]String", "Twelve-String", name)
            name = re.sub(r"13[- ]String", "Thirteen-String", name)
            name = re.sub(r"17[- ]String", "Seventeen-String", name)
            name = re.sub(r"18[- ]String", "Eighteen-String", name)
            name = re.sub(r"1[- ]String", "One-String", name)
            name = re.sub(r"2[- ]String", "Two-String", name)
            name = re.sub(r"3[- ]String", "Three-String", name)
            name = re.sub(r"4[- ]String", "Four-String", name)
            name = re.sub(r"5[- ]String", "Five-String", name)
            name = re.sub(r"6[- ]String", "Six-String", name)
            name = re.sub(r"7[- ]String", "Seven-String", name)
            name = re.sub(r"8[- ]String", "Eight-String", name)
            name = re.sub(r"9[- ]String", "Nine-String", name)

        # Ordering
        if match := re.search(r"^(.*) \(Six-String\)$", name):
            # print("  Ordering")
            name = "Six-String " + match.group(1)
        if match := re.search(r"^(.*) \(Twelve-String\)$", name):
            # print("  Ordering")
            name = "Twelve-String " + match.group(1)

        # Convert number of bits
        name = re.sub(r"8[- ]Bit ", "Eight-Bit ", name)
        name = re.sub(r"12[- ]Bit ", "Twelve-Bit ", name)
        name = re.sub(r"16[- ]Bit ", "Sixteen-Bit ", name)
        name = re.sub(r"24[- ]Bit ", "TwentyFour-Bit ", name)
        name = re.sub(r"32[- ]Bit ", "ThirtyTwo-Bit ", name)

        # Remove leading numbers
        if match := re.search(r"^\d+[ :]+(.*)$", name):
            # print("  Remove leading numbers")
            matched = match.group(1)
            if matched == "Tone Row":
                name = re.sub(r"12[- ]", "Twelve-", name)
            elif matched == "Track Recording":
                name = re.sub(r"16[- ]", "Sixteen-", name)
            else:
                name = match.group(1)

        # Numbers or dates in brackets
        name = re.sub(r"^\(\d+[- ]*\d*\)", "", name)
        name = re.sub(r"^\(\d+[- ]*Present\)", "", name)
        name = re.sub(r"^\d\d\d\d[- ]+\d\d\d\d", "", name)
        name = re.sub(r"^\d\d\d\d[- ]+\d\d[- ]\d\d", "", name)
        name = re.sub(r"©\s*\d\d\d\d", "", name)
        name = re.sub(r"℗\s*\d\d\d\d", "", name)

        # Vocals
        # name = re.sub(r"^\s*Voix$", "Voice", name, flags=re.IGNORECASE)

        # 1st 2nd etc
        # name = re.sub(r"^1st-", "First-", name, flags=re.IGNORECASE)
        name = re.sub(r"^1st\s*", "", name, flags=re.IGNORECASE)
        # name = re.sub(r"^2nd-", "Second-", name, flags=re.IGNORECASE)
        name = re.sub(r"^2nd\s*", "", name, flags=re.IGNORECASE)
        # name = re.sub(r"^3rd-", "Third-", name, flags=re.IGNORECASE)
        name = re.sub(r"^3rd\s*", "", name, flags=re.IGNORECASE)
        # name = re.sub(r"^4th-", "Forth-", name, flags=re.IGNORECASE)
        name = re.sub(r"^4th\s*", "", name, flags=re.IGNORECASE)

        # By
        name = re.sub(r"^.*Recorded By", "Recorded By", name, flags=re.IGNORECASE)
        name = re.sub(r"^.*Written By", "Written By", name, flags=re.IGNORECASE)

        # Abbreviations
        name = re.sub(r"\bMus[. ]", "Musicians ", name)
        name = re.sub(r"\bArr[. ]", "Arranged ", name)
        name = re.sub(r"\bDir[. ]", "Director ", name)
        name = re.sub(r"\bFea[. ]", "Featuring ", name)
        name = re.sub(r"\bFeat[. ]", "Featuring ", name)
        name = re.sub(r"\bTrad[. ]", "Traditional ", name)
        # name = re.sub(r"\bArr\.", " Arranged", name)
        # name = re.sub(r"\bArr[. ]", " Arranged ", name)
        name = re.sub(r"\bProd[. ]+By", " Produced By", name)
        name = re.sub(r"\bProd[. ]", " Producer ", name)
        name = re.sub(r"\bAcc[. ]", "Acoustic ", name)
        name = re.sub(r"\bAdd[. ]", "Additional ", name)
        name = re.sub(r"\bAssoc[. ]", "Associate ", name)
        name = re.sub(r"\bAsst[. ]", "Assistant ", name)
        name = re.sub(r"\bExec[. ]", "Executive ", name)
        name = re.sub(r"\bOrc[. ]", "Orchestral ", name)
        name = re.sub(r"\bBck[. ]", "Backing ", name)
        name = re.sub(r"\bBkg[. ]", "Backing ", name)
        name = re.sub(r"\bBk[. ]", "Backing ", name)
        name = re.sub(r"\bMod[. ]", "Moderno ", name)
        name = re.sub(r"\bMisc([. ]|$)", "Miscellaneous ", name)
        name = re.sub(r"\bFx([. ]|$)", "Effects ", name, flags=re.IGNORECASE)
        # name = re.sub(r"^Fx\b", "Effects ", name, flags=re.IGNORECASE)

        # Translations
        name = re.sub(r"作曲", "Composition", name)
        name = re.sub(r"編曲", "Arrangement", name)
        name = re.sub(r"作詞", "Lyrics", name)
        name = re.sub(r"譜本", "Music", name)

        # Initialisms eg. A.B.C.
        if match := re.search(r"^(\S)\.(\S)\. *$", name):
            # print("  Initialism")
            name = match.group(1).upper() + match.group(2).upper()
        if match := re.search(r"^(\S)\.(\S)\.(\S)\. *$", name):
            # print("  Initialism")
            name = (
                match.group(1).upper() + match.group(2).upper() + match.group(3).upper()
            )
        if match := re.search(r"^(\S)\.(\S)\.(\S)\.(\S)\. *$", name):
            # print("  Initialism")
            name = (
                match.group(1).upper()
                + match.group(2).upper()
                + match.group(3).upper()
                + match.group(4).upper()
            )

        # Odd characters
        name = re.sub(r" {2}", " ", name)
        name = re.sub(r" , ", ", ", name)
        name = re.sub(r"[:`.]+$", "", name)
        name = re.sub(r" Etc$", "", name)
        name = re.sub(r"-$", "", name)
        name = re.sub(r"–$", "", name)
        name = re.sub(r"^\*", "", name)
        name = re.sub(r"\*$", "", name)
        name = re.sub(r";$", "", name)
        name = re.sub(r"\)$", "", name)
        name = re.sub(r"]$", "", name)
        name = re.sub(r"],", ",", name)
        name = re.sub(r"] ", " ", name)

        # A&R change back to avoid splitting
        name = re.sub(r"\bANR\b", "A&R", name)

        # Remove leading and trailing spaces
        name = name.strip()

        # print(f"    {name}")
        return name

    @staticmethod
    def role_name_lookup(role_name: str) -> tuple[str, int] | None:
        if role_name in RoleDataAccess.role_name_set:
            return role_name, 100
        else:
            # Match lower case versions, but return correct name
            role_name_lower = role_name.lower()
            for name in RoleDataAccess.role_name_set:
                if role_name_lower == name.lower():
                    return name, 100
        return None

    @staticmethod
    def role_name_fuzzy_lookup(role_name: str) -> tuple[str, int] | None:
        top_role_name = process.extractOne(
            role_name,
            RoleDataAccess.role_name_set,
            # score_hint=90,
        )
        # print(f"{role_name} -> {top_role_name}")
        if top_role_name[1] > 90:
            # print(f"{int(top_role_name[1])} {role_name} -> {top_role_name}")
            return top_role_name[0], int(top_role_name[1])
        else:
            return None

    @staticmethod
    def find_role(role_name: str) -> str | None:
        # Role name has already been normalised

        # Keep track of best candidate so far
        top_candidate = ""
        top_score = 0

        # Using a breadth-first breakdown of words in role_name
        queue = deque()
        queue.append(role_name)

        while queue:
            queued_role_name: str = queue.popleft()
            # print(f"{queued_role_name}")

            # find if we have a match
            found_role_name = RoleDataAccess.find_role_inner(queued_role_name)
            if found_role_name is not None:
                if found_role_name[1] > top_score:
                    top_candidate = found_role_name[0]
                    top_score = found_role_name[1]

            # Remove each word in turn and add to queue
            word_list = queued_role_name.split(" ")
            if len(word_list) < 5:
                # Start from far end
                word_list.reverse()
                for word in word_list:
                    # Remove word
                    processed_role_name = queued_role_name.replace(word, "")
                    processed_role_name = re.sub(r" {2}", " ", processed_role_name)
                    processed_role_name = processed_role_name.strip()

                    if processed_role_name != "":
                        # Add to queue
                        queue.append(processed_role_name)
            else:
                # Long sentence makes processing too complex, so split in two
                word_list_part_one = word_list[: len(word_list) // 2]
                word_list_part_two = word_list[len(word_list) // 2 :]
                part_1 = " ".join(word_list_part_one)
                part_2 = " ".join(word_list_part_two)
                queue.append(part_1)
                queue.append(part_2)

        if top_candidate != "" and top_score > 90:
            return top_candidate
        else:
            log.debug(f"role not found: {role_name}")
            return None

    @staticmethod
    @functools.lru_cache(maxsize=100000)
    def find_role_inner(role_name: str) -> tuple[str, int] | None:
        if role_name is None or role_name == "":
            return None

        role_name = RoleDataAccess.substitute_role_alternatives(role_name)

        lookup_result = RoleDataAccess.role_name_lookup(role_name)
        if lookup_result is not None:
            return lookup_result

        top_role_name = RoleDataAccess.role_name_fuzzy_lookup(role_name)
        if top_role_name is not None:
            return top_role_name

        return None

    @staticmethod
    def substitute_role_alternatives(role_name: str) -> str:
        # Replacements
        role_name_lower = role_name.lower()
        if role_name_lower == "art":
            role_name = "Artwork"
        elif role_name_lower == "dj":
            role_name = "DJ"
        elif role_name_lower == "cgi":
            role_name = "CGI"
        elif role_name_lower == "dj mix":
            role_name = "DJ Mix"
        elif role_name_lower == "cgi artist":
            role_name = "CGI Artist"
        elif role_name_lower == "vibes":
            role_name = "Vibraphone"
        elif role_name_lower == "remiz":
            role_name = "Remix"
        elif role_name_lower == "mixing":
            role_name = "Mixed By"
        elif (
            role_name_lower == "singer"
            or role_name_lower == "vox"
            or role_name_lower == "vocalist"
        ):
            role_name = "Vocals"
        elif role_name_lower == "rythm":
            role_name = "Rhythm"

        return role_name
