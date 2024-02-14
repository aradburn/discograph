import enum


class EntityType(enum.Enum):
    ARTIST = 1
    LABEL = 2

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented
