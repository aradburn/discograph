import enum

import peewee


class EnumField(peewee.IntegerField):
    # noinspection PyMissingConstructor
    def __init__(self, choices, *args, **kwargs):
        super(peewee.IntegerField, self).__init__(*args, **kwargs)
        self.choices = choices

    def db_value(self, value: enum.Enum):
        if type(value) is int:
            return value
        else:
            return value.value

    def python_value(self, value):
        return self.choices(type(list(self.choices)[0].value)(value))
