import functools
import re
import typing

import grapheme

ConfigAllowedTypes = typing.Union[tuple, list, str, int, bool, None]

VALIDATORS_TRANSLATIONS = {
    "digits": " with exactly {digits} digits",
    "integer_min": "{sign}integer greater than {minimum}{digits}",
    "integer_range": "{sign}integer from {minimum} to {maximum}{digits}",
    "integer": "{sign}integer{digits}",
    "integer_max": "{sign}integer less than {maximum}{digits}",
    "each": " (each must be {each})",
    "fixed_len": " (exactly {fixed_len} pcs.)",
    "max_len": " (up to {max_len} pcs.)",
    "len_range": " (from {min_len} to {max_len} pcs.)",
    "min_len": " (at least {min_len} pcs.)",
    "series": "series of values{len}{each}, separated with «,»",
    "string_fixed_len": "string of length {length}",
    "string": "string",
    "string_max_len": "string of length up to {max_len}",
    "string_len_range": "string of length from {min_len} to {max_len}",
    "string_min_len": "string of length at least {min_len}",
    "regex": "string matching pattern «{regex}»",
    "float_min": "{sign}float greater than {minimum}",
    "float_range": "{sign}float from {minimum} to {maximum}",
    "float": "{sign}float",
    "float_max": "{sign}float less than {maximum}",
    "entity_like": "link to entity, username or Telegram ID",
}


class ValidationError(Exception):
    """
    Is being raised when config value passed can't be converted properly
    Must be raised with string, describing why value is incorrect
    It will be shown in .config, if user tries to set incorrect value
    """


class Validator:
    """Class used as validator of config value"""

    def __init__(
        self,
        validator: callable,
        doc: typing.Optional[typing.Union[str, dict]] = None,
        _internal_id: typing.Optional[int] = None,
    ):
        self.validate = validator

        if isinstance(doc, str):
            doc = {"en": doc}
        self.doc = doc
        self.internal_id = _internal_id


class Boolean(Validator):
    """
    Any logical value to be passed
    `1`, `"1"` etc. will be automatically converted to bool
    """

    def __init__(self):
        super().__init__(
            self._validate,
            {"en": "boolean"},
            _internal_id="Boolean",
        )

    @staticmethod
    def _validate(value: ConfigAllowedTypes, /) -> bool:
        true = ["True", "true", "1", 1, True, "yes", "Yes", "on", "On", "y", "Y"]
        false = ["False", "false", "0", 0, False, "no", "No", "off", "Off", "n", "N"]
        if value not in true + false:
            raise ValidationError("Passed value must be a boolean")
        return value in true


class Integer(Validator):
    """Checks whether passed argument is an integer value"""

    def __init__(
        self,
        *,
        digits: typing.Optional[int] = None,
        minimum: typing.Optional[int] = None,
        maximum: typing.Optional[int] = None,
    ):
        _signs = {}
        if minimum is not None and minimum == 0:
            _signs = {"en": "positive"}
        elif maximum is not None and maximum == 0:
            _signs = {"en": "negative"}
        else:
            _signs = {}
        _digits = {}
        if digits is not None:
            _digits = {"en": VALIDATORS_TRANSLATIONS["digits"].format(digits=digits)}
        else:
            _digits = {}
        if minimum is not None and minimum != 0:
            doc = (
                {
                    "en": VALIDATORS_TRANSLATIONS["integer_min"].format(
                        sign=_signs.get("en", ""),
                        digits=_digits.get("en", ""),
                        minimum=minimum,
                    )
                }
                if maximum is None and maximum != 0
                else {
                    "en": VALIDATORS_TRANSLATIONS["integer_range"].format(
                        sign=_signs.get("en", ""),
                        digits=_digits.get("en", ""),
                        minimum=minimum,
                        maximum=maximum,
                    )
                }
            )
        elif maximum is None and maximum != 0:
            doc = {
                "en": VALIDATORS_TRANSLATIONS["integer"].format(
                    sign=_signs.get("en", ""),
                    digits=_digits.get("en", ""),
                )
            }
        else:
            doc = {
                "en": VALIDATORS_TRANSLATIONS["integer_max"].format(
                    sign=_signs.get("en", ""),
                    digits=_digits.get("en", ""),
                    maximum=maximum,
                )
            }
        super().__init__(
            functools.partial(
                self._validate,
                digits=digits,
                minimum=minimum,
                maximum=maximum,
            ),
            doc,
            _internal_id="Integer",
        )

    @staticmethod
    def _validate(
        value: ConfigAllowedTypes,
        /,
        *,
        digits: int,
        minimum: int,
        maximum: int,
    ) -> typing.Union[int, None]:
        try:
            value = int(str(value).strip())
        except ValueError:
            raise ValidationError(f"Passed value ({value}) must be a number")
        if minimum is not None and value < minimum:
            raise ValidationError(f"Passed value ({value}) is lower than minimum one")
        if maximum is not None and value > maximum:
            raise ValidationError(f"Passed value ({value}) is greater than maximum one")
        if digits is not None and len(str(value)) != digits:
            raise ValidationError(
                f"The length of passed value ({value}) is incorrect "
                f"(Must be exactly {digits} digits)"
            )
        return value


class Series(Validator):
    """Represents the series of value (simply `list`)"""

    def __init__(
        self,
        validator: typing.Optional[Validator] = None,
        min_len: typing.Optional[int] = None,
        max_len: typing.Optional[int] = None,
        fixed_len: typing.Optional[int] = None,
    ):
        def trans(lang: str) -> str:
            return validator.doc.get("en", validator.doc["en"])

        _each = (
            {"en": VALIDATORS_TRANSLATIONS["each"].format(each=trans("en"))}
            if validator is not None
            else {}
        )

        if fixed_len is not None:
            _len = {
                "en": VALIDATORS_TRANSLATIONS["fixed_len"].format(fixed_len=fixed_len)
            }
        elif min_len is None:
            if max_len is None:
                _len = {}
            else:
                _len = {
                    "en": VALIDATORS_TRANSLATIONS["max_len"].format(max_len=max_len)
                }
        elif max_len is not None:
            _len = {
                "en": VALIDATORS_TRANSLATIONS["len_range"].format(
                    min_len=min_len, max_len=max_len
                )
            }
        else:
            _len = {"en": VALIDATORS_TRANSLATIONS["min_len"].format(min_len=min_len)}
        super().__init__(
            functools.partial(
                self._validate,
                validator=validator,
                min_len=min_len,
                max_len=max_len,
                fixed_len=fixed_len,
            ),
            {
                "en": VALIDATORS_TRANSLATIONS["series"].format(
                    each=_each.get("en", ""), len=_len.get("en", "")
                )
            },
            _internal_id="Series",
        )

    @staticmethod
    def _validate(
        value: ConfigAllowedTypes,
        /,
        *,
        validator: typing.Optional[Validator] = None,
        min_len: typing.Optional[int] = None,
        max_len: typing.Optional[int] = None,
        fixed_len: typing.Optional[int] = None,
    ) -> typing.List[ConfigAllowedTypes]:
        if not isinstance(value, (list, tuple, set)):
            value = str(value).split(",")
        if isinstance(value, (tuple, set)):
            value = list(value)
        if min_len is not None and len(value) < min_len:
            raise ValidationError(
                f"Passed value ({value}) contains less than {min_len} items"
            )
        if max_len is not None and len(value) > max_len:
            raise ValidationError(
                f"Passed value ({value}) contains more than {max_len} items"
            )
        if fixed_len is not None and len(value) != fixed_len:
            raise ValidationError(
                f"Passed value ({value}) must contain exactly {fixed_len} items"
            )
        value = [item.strip() if isinstance(item, str) else item for item in value]

        if isinstance(validator, Validator):
            for i, item in enumerate(value):
                try:
                    value[i] = validator.validate(item)
                except ValidationError:
                    raise ValidationError(
                        f"Passed value ({value}) contains invalid item"
                        f" ({str(item).strip()}), which must be {validator.doc['en']}"
                    )
        value = list(filter(lambda x: x, value))

        return value


class String(Validator):
    """Checks for length of passed value and automatically converts it to string"""

    def __init__(
        self,
        length: typing.Optional[int] = None,
        min_len: typing.Optional[int] = None,
        max_len: typing.Optional[int] = None,
    ):
        if length is not None:
            doc = {
                "en": VALIDATORS_TRANSLATIONS["string_fixed_len"].format(length=length)
            }
        else:
            if min_len is None:
                if max_len is None:
                    doc = {"en": VALIDATORS_TRANSLATIONS["string"]}
                else:
                    doc = {
                        "en": VALIDATORS_TRANSLATIONS["string_max_len"].format(
                            max_len=max_len
                        )
                    }
            elif max_len is not None:
                doc = {
                    "en": VALIDATORS_TRANSLATIONS["string_len_range"].format(
                        min_len=min_len, max_len=max_len
                    )
                }
            else:
                doc = {
                    "en": VALIDATORS_TRANSLATIONS["string_min_len"].format(
                        min_len=min_len
                    )
                }
        super().__init__(
            functools.partial(
                self._validate,
                length=length,
                min_len=min_len,
                max_len=max_len,
            ),
            doc,
            _internal_id="String",
        )

    @staticmethod
    def _validate(
        value: ConfigAllowedTypes,
        /,
        *,
        length: typing.Optional[int],
        min_len: typing.Optional[int],
        max_len: typing.Optional[int],
    ) -> str:
        if (
            isinstance(length, int)
            and len(list(grapheme.graphemes(str(value)))) != length
        ):
            raise ValidationError(
                f"Passed value ({value}) must be a length of {length}"
            )
        if (
            isinstance(min_len, int)
            and len(list(grapheme.graphemes(str(value)))) < min_len
        ):
            raise ValidationError(
                f"Passed value ({value}) must be a length of at least {min_len}"
            )
        if (
            isinstance(max_len, int)
            and len(list(grapheme.graphemes(str(value)))) > max_len
        ):
            raise ValidationError(
                f"Passed value ({value}) must be a length of up to {max_len}"
            )
        return str(value)


class RegExp(Validator):
    """Checks if value matches the regex"""

    def __init__(
        self,
        regex: str,
        flags: typing.Optional[re.RegexFlag] = None,
        description: typing.Optional[typing.Union[dict, str]] = None,
    ):
        if not flags:
            flags = 0
        try:
            re.compile(regex, flags=flags)
        except re.error as e:
            raise Exception(f"{regex} is not a valid regex") from e
        if description is None:
            doc = {"en": VALIDATORS_TRANSLATIONS["regex"].format(regex=regex)}
        else:
            if isinstance(description, str):
                doc = {"en": description}
            else:
                doc = description
        super().__init__(
            functools.partial(self._validate, regex=regex, flags=flags),
            doc,
            _internal_id="RegExp",
        )

    @staticmethod
    def _validate(
        value: ConfigAllowedTypes,
        /,
        *,
        regex: str,
        flags: typing.Optional[re.RegexFlag],
    ) -> str:
        if not re.match(regex, str(value), flags=flags):
            raise ValidationError(f"Passed value ({value}) must follow pattern {regex}")
        return str(value)


class Float(Validator):
    """Checks whether passed argument is a float value"""

    def __init__(
        self,
        minimum: typing.Optional[float] = None,
        maximum: typing.Optional[float] = None,
    ):
        _signs = {}
        if minimum is not None and minimum == 0:
            _signs = {"en": "positive"}
        elif maximum is not None and maximum == 0:
            _signs = {"en": "negative"}
        else:
            _signs = {}
        if minimum is not None and minimum != 0:
            doc = (
                {
                    "en": VALIDATORS_TRANSLATIONS["float_min"].format(
                        sign=_signs.get("en", ""), minimum=minimum
                    )
                }
                if maximum is None and maximum != 0
                else {
                    "en": VALIDATORS_TRANSLATIONS["float_range"].format(
                        sign=_signs.get("en", ""), minimum=minimum, maximum=maximum
                    )
                }
            )
        elif maximum is None and maximum != 0:
            doc = {
                "en": VALIDATORS_TRANSLATIONS["float"].format(sign=_signs.get("en", ""))
            }
        else:
            doc = {
                "en": VALIDATORS_TRANSLATIONS["float_max"].format(
                    sign=_signs.get("en", ""), maximum=maximum
                )
            }
        super().__init__(
            functools.partial(
                self._validate,
                minimum=minimum,
                maximum=maximum,
            ),
            doc,
            _internal_id="Float",
        )

    @staticmethod
    def _validate(
        value: ConfigAllowedTypes,
        /,
        *,
        minimum: typing.Optional[float] = None,
        maximum: typing.Optional[float] = None,
    ) -> float:
        try:
            value = float(str(value).strip().replace(",", "."))
        except ValueError:
            raise ValidationError(f"Passed value ({value}) must be a float")
        if minimum is not None and value < minimum:
            raise ValidationError(f"Passed value ({value}) is lower than minimum one")
        if maximum is not None and value > maximum:
            raise ValidationError(f"Passed value ({value}) is greater than maximum one")
        return value


class EntityLike(RegExp):
    def __init__(self):
        super().__init__(
            regex=r"^(?:@|https?://t\.me/)?(?:[a-zA-Z0-9_]{5,32}|[a-zA-Z0-9_]{1,32}\?[a-zA-Z0-9_]{1,32})$",
            description={"en": VALIDATORS_TRANSLATIONS["entity_like"]},
        )

    @staticmethod
    def _validate(
        value: ConfigAllowedTypes,
        /,
        *,
        regex: str,
        flags: typing.Optional[re.RegexFlag],
    ) -> typing.Union[str, int]:
        value = super()._validate(value, regex=regex, flags=flags)

        if value.isdigit():
            if value.startswith("-100"):
                value = value[4:]
            value = int(value)
        if value.startswith("https://t.me/"):
            value = value.split("https://t.me/")[1]
        if not value.startswith("@"):
            value = f"@{value}"
        return value
