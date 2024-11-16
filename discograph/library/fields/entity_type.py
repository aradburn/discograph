import enum


class EntityType(enum.Enum):
    ARTIST = 1
    LABEL = 2

    @staticmethod
    def from_str(entity_type_str: str):
        if entity_type_str in ("artist", "ARTIST"):
            return EntityType.ARTIST
        elif entity_type_str in ("label", "LABEL"):
            return EntityType.LABEL
        else:
            raise NotImplementedError

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented

    def __repr__(self):
        return self.name
