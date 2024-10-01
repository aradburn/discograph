import collections

from discograph.library.data_access_layer.role_data_access import RoleDataAccess


class RoleEntry:
    # CLASS VARIABLES

    # INITIALIZER

    def __init__(self, name=None, detail=None):
        self._name = name
        self._detail = detail

    def __eq__(self, other):
        return self._name == other.name and self._detail == other.detail

    # PUBLIC METHODS

    @classmethod
    def from_element(cls, element):
        credit_roles = []
        if element is None or not element.text:
            return credit_roles
        current_text = ""
        bracket_depth = 0
        for character in element.text:
            if character == "[":
                bracket_depth += 1
            elif character == "]":
                bracket_depth -= 1
            elif not bracket_depth and character == ",":
                current_text = current_text.strip()
                if current_text:
                    credit_roles.append(cls.from_text(current_text))
                current_text = ""
                continue
            current_text += character
        current_text = current_text.strip()
        if current_text:
            credit_roles.append(cls.from_text(current_text))
        return credit_roles

    @classmethod
    def from_text(cls, text):
        role_name = ""
        current_buffer = ""
        details = []
        had_detail = False
        bracket_depth = 0
        for character in text:
            if character == "[":
                bracket_depth += 1
                if bracket_depth == 1 and not had_detail:
                    role_name = current_buffer
                    current_buffer = ""
                    had_detail = True
                elif 1 < bracket_depth:
                    current_buffer += character
            elif character == "]":
                bracket_depth -= 1
                if not bracket_depth:
                    details.append(current_buffer)
                    current_buffer = ""
                else:
                    current_buffer += character
            else:
                current_buffer += character
        if current_buffer and not had_detail:
            role_name = current_buffer
        role_name = role_name.strip()
        role_detail = ", ".join(_.strip() for _ in details)
        role_detail = role_detail or None
        return cls(name=role_name, detail=role_detail)

    @classmethod
    def get_multiselect_mapping(cls):
        # excluded_roles = [
        #    'Alias',
        #    'Member Of',
        #    ]
        mapping = collections.OrderedDict()
        for role_name in sorted(RoleDataAccess.role_name_to_role_id_lookup.keys()):
            role_id = RoleDataAccess.role_name_to_role_id_lookup[role_name]
            role_category = RoleDataAccess.role_id_to_role_category_lookup[role_id]

            if role_category not in mapping:
                mapping[role_category] = []
            mapping[role_category].append(role_name)
        return mapping

    # PUBLIC PROPERTIES

    @property
    def detail(self):
        return self._detail

    @property
    def name(self):
        return self._name
