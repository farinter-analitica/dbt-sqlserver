from dbt.adapters.fabric import FabricColumn


class SQLServerColumn(FabricColumn):
    def is_string(self) -> bool:
        return super().is_string() or self.dtype.lower() in [
            # real types
            "char",
            "varchar",
            "text",
            "nchar",
            "nvarchar",
            "ntext",
            "uniqueidentifier",
            # aliases
            "string",
            "str",
            "stringtype",
        ]
    
    def is_integer(self) -> bool:
        return self.dtype.lower() in [
            # real types
            "smallint",
            "integer",
            "bigint",
            "smallserial",
            "serial",
            "bigserial",
            # aliases
            "int2",
            "int4",
            "int8",
            "serial2",
            "serial4",
            "serial8",
            "int",
        ]
