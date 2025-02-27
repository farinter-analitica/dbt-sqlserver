import base64
import hashlib
import math
import re
from abc import ABC, abstractmethod
from functools import lru_cache
from typing import (
    ClassVar,
    Sequence,
)
# taken from dlt source code
# from dlt.common.normalizers.naming.snake_case import NamingConvention as SnakeCase


class NamingConvention(ABC):
    """Initializes naming convention to generate identifier with `max_length` if specified. Base naming convention
    is case sensitive by default
    """

    _TR_TABLE: ClassVar[bytes] = bytes.maketrans(b"/+", b"ab")
    _DEFAULT_COLLISION_PROB: ClassVar[float] = 0.001
    PATH_SEPARATOR: ClassVar[str] = "__"
    """Subsequent nested fields will be separated with the string below, applies both to field and table names"""

    def __init__(self, max_length: int | None = None) -> None:
        self.max_length = max_length

    @property
    @abstractmethod
    def is_case_sensitive(self) -> bool:
        """Tells if given naming convention is producing case insensitive or case sensitive identifiers."""
        pass

    @abstractmethod
    def normalize_identifier(self, identifier: str) -> str:
        """Normalizes and shortens the identifier according to naming convention in this function code"""
        if identifier is None:
            raise ValueError("name is None")
        identifier = identifier.strip()
        if not identifier:
            raise ValueError(identifier)
        return identifier

    def normalize_table_identifier(self, identifier: str) -> str:
        """Normalizes and shortens identifier that will function as a dataset, table or schema name, defaults to `normalize_identifier`"""
        return self.normalize_identifier(identifier)

    def make_path(self, *identifiers: str) -> str:
        """Builds path out of identifiers. Identifiers are neither normalized nor shortened"""
        return self.PATH_SEPARATOR.join(filter(lambda x: x.strip(), identifiers))

    def break_path(self, path: str) -> Sequence[str]:
        """Breaks path into sequence of identifiers"""
        return [ident for ident in path.split(self.PATH_SEPARATOR) if ident.strip()]

    def normalize_path(self, path: str) -> str:
        """Breaks path into identifiers, normalizes components, reconstitutes and shortens the path"""
        normalized_idents = [
            self.normalize_identifier(ident) for ident in self.break_path(path)
        ]
        # shorten the whole path
        return self.shorten_identifier(
            self.make_path(*normalized_idents), path, self.max_length
        )

    def normalize_tables_path(self, path: str) -> str:
        """Breaks path of table identifiers, normalizes components, reconstitutes and shortens the path"""
        normalized_idents = [
            self.normalize_table_identifier(ident) for ident in self.break_path(path)
        ]
        # shorten the whole path
        return self.shorten_identifier(
            self.make_path(*normalized_idents), path, self.max_length
        )

    def shorten_fragments(self, *normalized_idents: str) -> str | None:
        """Reconstitutes and shortens the path of normalized identifiers"""
        if not normalized_idents:
            return None
        path_str = self.make_path(*normalized_idents)
        return self.shorten_identifier(path_str, path_str, self.max_length)

    @classmethod
    def name(cls) -> str:
        """Naming convention name is the name of the module in which NamingConvention is defined"""
        if cls.__module__.startswith("dlt.common.normalizers.naming."):
            # return last component
            return cls.__module__.split(".")[-1]
        return cls.__module__

    def __str__(self) -> str:
        name = self.name()
        name += "_cs" if self.is_case_sensitive else "_ci"
        if self.max_length:
            name += f"_{self.max_length}"
        return name

    @staticmethod
    @lru_cache(maxsize=None)
    def shorten_identifier(
        normalized_ident: str,
        identifier: str,
        max_length: int,
        collision_prob: float = _DEFAULT_COLLISION_PROB,
    ) -> str:
        """Shortens the `name` to `max_length` and adds a tag to it to make it unique. Tag may be placed in the middle or at the end"""
        if max_length and len(normalized_ident) > max_length:
            # use original identifier to compute tag
            tag = NamingConvention._compute_tag(identifier, collision_prob)
            normalized_ident = NamingConvention._trim_and_tag(
                normalized_ident, tag, max_length
            )

        return normalized_ident

    @staticmethod
    def _compute_tag(identifier: str, collision_prob: float) -> str:
        # assume that shake_128 has perfect collision resistance 2^N/2 then collision prob is 1/resistance: prob = 1/2^N/2, solving for prob
        # take into account that we are case insensitive in base64 so we need ~1.5x more bits (2+1)
        tl_bytes = int(((2 + 1) * math.log2(1 / (collision_prob)) // 8) + 1)
        tag = (
            base64.b64encode(
                hashlib.shake_128(identifier.encode("utf-8")).digest(tl_bytes)
            )
            .rstrip(b"=")
            .translate(NamingConvention._TR_TABLE)
            .lower()
            .decode("ascii")
        )
        return tag

    @staticmethod
    def _trim_and_tag(identifier: str, tag: str, max_length: int) -> str:
        assert len(tag) <= max_length
        remaining_length = max_length - len(tag)
        remaining_overflow = remaining_length % 2
        identifier = (
            identifier[: remaining_length // 2 + remaining_overflow]
            + tag
            + identifier[len(identifier) - remaining_length // 2 :]
        )
        assert len(identifier) == max_length
        return identifier


RE_UNDERSCORES = re.compile("__+")
RE_LEADING_DIGITS = re.compile(r"^\d+")
RE_ENDING_UNDERSCORES = re.compile(r"_+$")
RE_NON_ALPHANUMERIC = re.compile(r"[^a-zA-Z\d_]+")


class SnakeCase(NamingConvention):
    """Case insensitive naming convention, converting source identifiers into lower case snake case with reduced alphabet.

    - Spaces around identifier are trimmed
    - Removes all ascii characters except ascii alphanumerics and underscores
    - Prepends `_` if name starts with number.
    - Multiples of `_` are converted into single `_`.
    - Replaces all trailing `_` with `x`
    - Replaces `+` and `*` with `x`, `-` with `_`, `@` with `a` and `|` with `l`

    Uses __ as parent-child separator for tables and flattened column names.
    """

    RE_UNDERSCORES = RE_UNDERSCORES
    RE_LEADING_DIGITS = RE_LEADING_DIGITS
    RE_NON_ALPHANUMERIC = RE_NON_ALPHANUMERIC

    _SNAKE_CASE_BREAK_1 = re.compile("([^_])([A-Z][a-z]+)")
    _SNAKE_CASE_BREAK_2 = re.compile("([a-z0-9])([A-Z])")
    _REDUCE_ALPHABET = ("+-*@|", "x_xal")
    _TR_REDUCE_ALPHABET = str.maketrans(_REDUCE_ALPHABET[0], _REDUCE_ALPHABET[1])

    @property
    def is_case_sensitive(self) -> bool:
        return False

    def normalize_identifier(self, identifier: str) -> str:
        identifier = super().normalize_identifier(identifier)
        # print(f"{identifier} -> {self.shorten_identifier(identifier, self.max_length)} ({self.max_length})")
        return self._normalize_identifier(identifier, self.max_length)

    @staticmethod
    @lru_cache(maxsize=None)
    def _normalize_identifier(identifier: str, max_length: int) -> str:
        """Normalizes the identifier according to naming convention represented by this function"""
        # all characters that are not letters digits or a few special chars are replaced with underscore
        normalized_ident = identifier.translate(SnakeCase._TR_REDUCE_ALPHABET)
        normalized_ident = SnakeCase.RE_NON_ALPHANUMERIC.sub("_", normalized_ident)

        # shorten identifier
        return SnakeCase.shorten_identifier(
            SnakeCase._to_snake_case(normalized_ident), identifier, max_length
        )

    @classmethod
    def _to_snake_case(cls, identifier: str) -> str:
        # then convert to snake case
        identifier = cls._SNAKE_CASE_BREAK_1.sub(r"\1_\2", identifier)
        identifier = cls._SNAKE_CASE_BREAK_2.sub(r"\1_\2", identifier).lower()

        # leading digits will be prefixed (if regex is defined)
        if cls.RE_LEADING_DIGITS and cls.RE_LEADING_DIGITS.match(identifier):
            identifier = "_" + identifier

        # replace trailing _ with x
        stripped_ident = identifier.rstrip("_")
        strip_count = len(identifier) - len(stripped_ident)
        stripped_ident += "x" * strip_count

        # identifier = cls._RE_ENDING_UNDERSCORES.sub("x", identifier)
        # replace consecutive underscores with single one to prevent name collisions with PATH_SEPARATOR
        return cls.RE_UNDERSCORES.sub("_", stripped_ident)
