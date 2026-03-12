from typing import Any, ClassVar, Dict

from dbt.adapters.base.column import Column
from dbt_common.exceptions import DbtRuntimeError


class SQLServerColumn(Column):
    TYPE_LABELS: ClassVar[Dict[str, str]] = {
        "STRING": "VARCHAR(8000)",
        "VARCHAR": "VARCHAR(8000)",
        "CHAR": "CHAR(1)",
        "NCHAR": "NCHAR(1)",
        "NVARCHAR": "NVARCHAR(4000)",
        "TIMESTAMP": "DATETIME2(6)",
        "DATETIME2": "DATETIME2(6)",
        "DATETIME2(6)": "DATETIME2(6)",
        "DATE": "DATE",
        "TIME": "TIME(6)",
        "FLOAT": "FLOAT",
        "REAL": "REAL",
        "INT": "INT",
        "INTEGER": "INT",
        "BIGINT": "BIGINT",
        "SMALLINT": "SMALLINT",
        "TINYINT": "SMALLINT",
        "BIT": "BIT",
        "BOOLEAN": "BIT",
        "DECIMAL": "DECIMAL",
        "NUMERIC": "NUMERIC",
        "MONEY": "DECIMAL",
        "SMALLMONEY": "DECIMAL",
        "UNIQUEIDENTIFIER": "UNIQUEIDENTIFIER",
        "VARBINARY": "VARBINARY(MAX)",
        "BINARY": "BINARY(1)",
    }

    @classmethod
    def string_type(cls, size: int) -> str:
        """Class-level string_type used by SQLAdapter.expand_column_types.

        Return a VARCHAR default for the SQLAdapter path; this keeps behaviour
        consistent with the rest of dbt where class-level string_type is
        generic and not instance-aware.
        """
        return f"varchar({size if size > 0 else '8000'})"

    def string_type_instance(self, size: int) -> str:
        """
        Instance-level string type selection that respects NVARCHAR/NCHAR.
        """
        dtype = (self.dtype or "").lower()
        if dtype == "nvarchar":
            return f"nvarchar({size if size > 0 else '4000'})"
        if dtype == "nchar":
            return f"nchar({size if size > 0 else '1'})"
        # default to varchar/char behaviour
        return f"varchar({size if size > 0 else '8000'})"

    def literal(self, value: Any) -> str:
        return "cast('{}' as {})".format(value, self.data_type)

    @property
    def data_type(self) -> str:
        # Always enforce datetime2 precision
        if self.dtype.lower() == "datetime2":
            return "datetime2(6)"
        if self.is_string():
            return self.string_type_instance(self.string_size())
        elif self.is_numeric():
            return self.numeric_type(self.dtype, self.numeric_precision, self.numeric_scale)
        else:
            return self.dtype

    def is_string(self) -> bool:
        return self.dtype.lower() in ["varchar", "char", "nvarchar", "nchar"]

    def is_number(self):
        return any(
            [self.is_integer(), self.is_numeric(), self.is_float(), self.is_fixed_numeric()]
        )

    def is_float(self):
        return self.dtype.lower() in ["float", "real"]

    def is_integer(self) -> bool:
        # Treat BIT as an integer-like type so it participates in integer
        # promotions (bit -> tinyint -> smallint -> int -> bigint).
        return self.dtype.lower() in ["int", "integer", "bigint", "smallint", "tinyint", "bit"]

    def is_numeric(self) -> bool:
        return self.dtype.lower() in ["numeric", "decimal"]

    def is_fixed_numeric(self) -> bool:
        return self.dtype.lower() in ["money", "smallmoney"]

    def string_size(self) -> int:
        if not self.is_string():
            raise DbtRuntimeError("Called string_size() on non-string field!")
        if self.char_size is None:
            return 8000
        else:
            return int(self.char_size)

    def can_expand_to(
        self, other_column: Column, enable_safe_type_expansion: bool = False
    ) -> bool:
        # If both are strings, allow size-based expansion regardless of the
        # feature flag. Only allow family changes (VARCHAR -> NVARCHAR) when
        # `sqlserver__enable_safe_type_expansion` is set by the adapter.
        self_dtype = self.dtype.lower()
        other_dtype = other_column.dtype.lower()
        if self.is_string() and other_column.is_string():
            self_size = self.string_size()
            other_size = other_column.string_size()

            if other_size > self_size and self_dtype == other_dtype:
                return True

            # Allow safe conversions across the CHAR/VARCHAR -> NCHAR/NVARCHAR family
            # only when the feature flag is enabled. Do NOT allow shrinking
            # conversions or NVARCHAR -> VARCHAR.
            if self_dtype in ("varchar", "char") and other_dtype in ("nvarchar", "nchar"):
                # allow when target has at least the same character capacity
                if other_size >= self_size and enable_safe_type_expansion:
                    return True

            # If none of the string rules matched, we can't expand.
            return False

        # If we reach here, at least one side is not a string. Apply integer/
        # numeric promotion logic only if the adapter has enabled type expansion.
        if not enable_safe_type_expansion or not self.is_number() or not other_column.is_number():
            return False

        # Integer family promotions (tinyint -> smallint -> int -> bigint)
        int_family = ("bit", "tinyint", "smallint", "int", "bigint")
        if self_dtype in int_family and other_dtype in int_family:
            if int_family.index(other_dtype) > int_family.index(self_dtype):
                return True

        self_prec = int(self.numeric_precision or 0)
        other_prec = int(other_column.numeric_precision or 0)
        # Integer -> numeric/decimal is a safe widening (integers fit in numerics).
        if self.is_integer() and other_column.is_numeric() and other_prec > self_prec:
            return True

        # Numeric/Decimal promotions: allow when target precision >= source precision
        # and target scale >= source scale (so we don't lose fractional digits).
        if (self.is_numeric() or self.is_fixed_numeric()) and (
            other_column.is_numeric() or other_column.is_fixed_numeric()
        ):
            # Access precision/scale directly from columns. Fall back to 0 when missing.
            self_scale = int(self.numeric_scale or 0)
            other_scale = int(other_column.numeric_scale or 0)

            if other_prec >= self_prec and other_scale >= self_scale:
                if other_prec > self_prec or other_scale > self_scale or self_dtype != other_dtype:
                    return True

        return False
