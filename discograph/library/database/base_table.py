from discograph import utils
from discograph.library.database.database_helper import Base


class BaseTable(Base):
    __abstract__ = True

    def __repr__(self):
        return utils.normalize_dict(utils.row2dict(self), skip_keys={"random"})

    # def __repr__(self, **fields: Any) -> str:
    #     """
    #     Helper for __repr__
    #     """
    #     field_strings = []
    #     at_least_one_attached_attribute = False
    #     for key, field in fields.items():
    #         try:
    #             field_strings.append(f"{key}={field!r}")
    #         except DetachedInstanceError:
    #             field_strings.append(f"{key}=DetachedInstanceError")
    #         else:
    #             at_least_one_attached_attribute = True
    #     if at_least_one_attached_attribute:
    #         return f"<{self.__class__.__name__}({','.join(field_strings)})>"
    #     return f"<{self.__class__.__name__} {id(self)}>"
