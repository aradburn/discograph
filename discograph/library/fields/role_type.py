import enum
import re


class RoleType:

    class Category(enum.Enum):
        ACTING_LITERARY_AND_SPOKEN = 1
        COMPANIES = 2
        CONDUCTING_AND_LEADING = 3
        DJ_MIX = 4
        FEATURING_AND_PRESENTING = 5
        INSTRUMENTS = 6
        MANAGEMENT = 7
        PRODUCTION = 8
        RELATION = 9
        REMIX = 10
        TECHNICAL = 11
        VISUAL = 12
        VOCAL = 13
        WRITING_AND_ARRANGEMENT = 14

    class Subcategory(enum.Enum):
        NONE = 0
        DRUMS_AND_PERCUSSION = 1
        KEYBOARD = 2
        OTHER_MUSICAL = 3
        STRINGED_INSTRUMENTS = 4
        TECHNICAL_MUSICAL = 5
        TUNED_PERCUSSION = 6
        WIND_INSTRUMENTS = 7

    _bracket_pattern = re.compile(r"\[(.+?)]")

    category_names = {
        Category.ACTING_LITERARY_AND_SPOKEN: "Acting, Literary & Spoken",
        Category.COMPANIES: "Companies",
        Category.CONDUCTING_AND_LEADING: "Conducting & Leading",
        Category.DJ_MIX: "DJ Mix",
        Category.FEATURING_AND_PRESENTING: "Featuring & Presenting",
        Category.INSTRUMENTS: "Instruments",
        Category.MANAGEMENT: "Management",
        Category.PRODUCTION: "Production",
        Category.RELATION: "Structural Relationships",
        Category.REMIX: "Remix",
        Category.TECHNICAL: "Technical",
        Category.VISUAL: "Visual",
        Category.VOCAL: "Vocal",
        Category.WRITING_AND_ARRANGEMENT: "Writing & Arrangement",
    }

    subcategory_names = {
        Subcategory.NONE: "None",
        Subcategory.DRUMS_AND_PERCUSSION: "Drums & Percussion",
        Subcategory.KEYBOARD: "Keyboard",
        Subcategory.OTHER_MUSICAL: "Other Musical",
        Subcategory.STRINGED_INSTRUMENTS: "String Instruments",
        Subcategory.TECHNICAL_MUSICAL: "Technical Musical",
        Subcategory.TUNED_PERCUSSION: "Tuned Percussion",
        Subcategory.WIND_INSTRUMENTS: "Wind Instruments",
    }

    aggregate_roles = (
        "Compiled By",
        "Curated By",
        "DJ Mix",
        "Hosted By",
        "Presenter",
    )

    @staticmethod
    def hornbostel_sachs_to_subcategory(
        hornbostel_sachs_classification: str,
    ) -> Subcategory:
        match hornbostel_sachs_classification.lower():
            case "idiophones":
                return RoleType.Subcategory.DRUMS_AND_PERCUSSION
            case "membranophones":
                return RoleType.Subcategory.DRUMS_AND_PERCUSSION
            case "chordophones":
                return RoleType.Subcategory.STRINGED_INSTRUMENTS
            case "aerophones":
                return RoleType.Subcategory.WIND_INSTRUMENTS
            case "electrophones":
                return RoleType.Subcategory.TECHNICAL_MUSICAL
            case _:
                return RoleType.Subcategory.OTHER_MUSICAL
