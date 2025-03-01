from collections import deque
from datetime import timedelta
import textwrap
from typing import Literal, Optional
from dagster import (
    AssetsDefinition,
    build_last_update_freshness_checks,
)
from dagster_shared_gf.shared_constants import (
    hourly_freshness_lbound_per_environ,
    RowTerminator,
)
from dagster_shared_gf.shared_functions import filter_assets_by_tags, get_now_datetime
from dagster_shared_gf.shared_variables import tags_repo
import polars as pl


def get_unique_source_assets(all_assets, source_assets):
    """
    Extracts unique source assets that don't overlap with existing asset keys.

    Args:
        all_assets: List of all asset definitions
        source_assets: List of source assets to filter

    Returns:
        List of source assets that don't have keys overlapping with all_assets
    """
    # Extract the asset keys from the AssetsDefinition instances
    all_asset_keys = {
        key
        for asset in all_assets
        if isinstance(asset, AssetsDefinition)
        for key in asset.keys
    }

    # Return source assets that don't overlap with existing keys
    return [
        source_asset
        for source_asset in source_assets
        if source_asset.key not in all_asset_keys
    ]


def create_freshness_checks_for_assets(all_assets):
    # Only collect materializable assets once
    materializable_assets = tuple(
        asset
        for asset in all_assets
        if isinstance(asset, AssetsDefinition) and asset.is_materializable
    )

    # First identify all assets by their highest frequency
    hourly_assets = filter_assets_by_tags(
        materializable_assets,
        tags_to_match=tags_repo.Hourly.tag,
        filter_type="any_tag_matches",
    )

    # Use set operations but only once per category
    remaining_assets = set(materializable_assets) - set(hourly_assets)

    daily_assets = filter_assets_by_tags(
        list(remaining_assets),  # Convert to list for filter_assets_by_tags
        tags_to_match=tags_repo.Daily.tag,
        filter_type="any_tag_matches",
    )

    remaining_assets = remaining_assets - set(daily_assets)

    weekly_assets = filter_assets_by_tags(
        list(remaining_assets),
        tags_to_match=tags_repo.Weekly.tag,
        filter_type="any_tag_matches",
    )

    remaining_assets = remaining_assets - set(weekly_assets)

    monthly_assets = filter_assets_by_tags(
        list(remaining_assets),
        tags_to_match=tags_repo.Monthly.tag,
        filter_type="any_tag_matches",
    )

    # Create freshness checks for all asset types
    hourly_freshness_checks = build_last_update_freshness_checks(
        assets=hourly_assets,
        lower_bound_delta=hourly_freshness_lbound_per_environ,
        deadline_cron="0 10-16 * * 1-6",
    )

    daily_freshness_checks = build_last_update_freshness_checks(
        assets=daily_assets,
        lower_bound_delta=timedelta(hours=26),
        deadline_cron="0 9 * * 1-6",
    )

    weekly_freshness_checks = build_last_update_freshness_checks(
        assets=weekly_assets,
        lower_bound_delta=timedelta(days=7, hours=2),  # Slightly over a week
        deadline_cron="0 9 * * 1",  # Monday mornings
    )

    monthly_freshness_checks = build_last_update_freshness_checks(
        assets=monthly_assets,
        lower_bound_delta=timedelta(days=31, hours=2),  # Slightly over a month
        deadline_cron="0 9 1 * *",  # First day of month
    )

    # Return all freshness checks
    return (
        *hourly_freshness_checks,
        *daily_freshness_checks,
        *weekly_freshness_checks,
        *monthly_freshness_checks,
    )


class SQLScriptGenerator:
    _df: pl.DataFrame
    _df_schema: pl.Schema
    _db_name: str | None
    _db_schema: str
    _table_name: str
    _primary_keys: tuple[str, ...] = tuple()
    _temp_table_name: Optional[str]
    _schema_table_relation: str
    _schema_temp_table_relation: str
    _full_relation: str | None = None
    _df_schema: pl.Schema
    _formatted_primary_keys: tuple[str, ...]

    def __init__(
        self,
        df: pl.DataFrame,
        db_schema: str,
        table_name: str,
        sql_lang: Literal["sqlserver"] = "sqlserver",
        db_name: str | None = None,
        primary_keys: tuple[str, ...] = tuple(),
        temp_table_name: Optional[str] = None,
    ):
        if sql_lang not in ["sqlserver"]:
            raise ValueError(f"SQL language {sql_lang} not implemented.")

        self._df = df
        self._df_schema = df.collect_schema()
        self._db_name = db_name
        self._db_schema = db_schema
        self._table_name = table_name
        self._temp_table_name = temp_table_name
        self._primary_keys = primary_keys
        self._schema_table_relation = f"[{self.db_schema}].[{self.table_name}]"
        self._full_relation = (
            f"[{self.db_name}].[{self.db_schema}].[{self.table_name}]"
            if db_name
            else None
        )
        self._schema_temp_table_relation = (
            f"[{self.db_schema}].[{self.temp_table_name}]"
        )
        self._formatted_primary_keys = self._validate_and_format_pks(
            columns=self.primary_keys
        )

    @property
    def df(self) -> pl.DataFrame:
        if self._df is None:
            raise ValueError("DataFrame has not been set")
        return self._df

    @property
    def df_schema(self) -> pl.Schema:
        if self._df_schema is None:
            raise ValueError("DataFrame schema has not been set")
        return self._df_schema

    @property
    def db_name(self) -> str:
        if not self._db_name:
            raise ValueError("db_name name has not been set")
        return self._db_name

    @property
    def db_schema(self) -> str:
        if not self._db_schema:
            raise ValueError("Database schema has not been set")
        return self._db_schema

    @property
    def table_name(self) -> str:
        if not self._table_name:
            raise ValueError("Table name has not been set")
        return self._table_name

    @property
    def primary_keys(self) -> tuple[str, ...]:
        return self._primary_keys

    @property
    def temp_table_name(self) -> str:
        return self._temp_table_name or self._table_name

    @property
    def schema_table_relation(self) -> str:
        return self._schema_table_relation

    @property
    def full_relation(self) -> str:
        if not self._full_relation:
            raise ValueError(
                "full_relation name has not been set, provide db_name and db_schema to set it"
            )
        return self._full_relation

    @property
    def schema_temp_table_relation(self) -> str:
        return self._schema_temp_table_relation

    @property
    def formatted_primary_keys(self) -> tuple[str, ...]:
        return self._formatted_primary_keys

    def _validate_and_format_pks(
        self, columns: tuple[str, ...] = tuple()
    ) -> tuple[str, ...]:
        # Get the schema of the DataFrame
        schema = self.df_schema

        # Check correct primary keys
        verified_columns = deque()
        formatted_columns = deque()

        for pk in columns:
            if pk not in schema:
                raise ValueError(
                    f"Primary key {pk} not in schema, available keys: {str(schema.names())}"
                )
            # Check for duplicates
            if pk in verified_columns:
                raise ValueError(f"Duplicate primary key: {pk}")
            # Validate not nulls
            column_data = self.df.get_column(pk)
            if column_data.has_nulls():
                raise ValueError(f"Primary key {pk} cannot be null")

            verified_columns.append(pk)
            formatted_columns.append(f"[{pk}]")

        # Check for data duplicates
        column_data = self.df.select(verified_columns).to_struct("primary_key")
        duplicates = column_data.filter(column_data.is_duplicated())
        if duplicates.len() > 0:
            raise ValueError(
                f"Primary key {str(verified_columns)} cannot have duplicates, found {str(duplicates.limit(10))}"
            )

        return tuple(formatted_columns)

    def create_table_sql_script(self, temp: bool = False) -> str:
        """
        Generate a SQL script to create a table based on a Polars DataFrame schema.

        Returns:
        - str: The SQL script to create the table.
        """
        schema = self.df_schema
        primary_keys = self.primary_keys
        df = self.df
        schema_table_relation = (
            self.schema_table_relation if not temp else self.schema_temp_table_relation
        )

        # Initialize the SQL script
        sql_script = f"CREATE TABLE {schema_table_relation} (\n"

        TYPE_MAPPING = {
            pl.Int8: "TINYINT",
            pl.Int16: "SMALLINT",
            pl.Int32: "INT",
            pl.Int64: "BIGINT",
            pl.Float32: "FLOAT(24)",
            pl.Float64: "FLOAT(53)",
            pl.Boolean: "BIT",
            pl.Date: "DATE",
            pl.UInt8: "TINYINT",
            pl.UInt16: "SMALLINT",
            pl.UInt32: "INT",
            pl.UInt64: "BIGINT",
        }

        # Iterate over the columns in the schema
        for col_name, col_type in schema.items():
            # Map the Polars data type to a SQL Server data type
            # Get the basic type mapping first
            sql_type = TYPE_MAPPING.get(col_type)

            # Handle special cases
            if sql_type is None:
                if col_type == pl.Utf8:
                    string_lenght = df.get_column(col_name).str.len_chars().max()  # type: ignore
                    string_lenght: int = string_lenght if string_lenght else 0
                    if col_name in primary_keys and not string_lenght > 50:
                        sql_type = "NVARCHAR(50)"
                    elif string_lenght <= 100:
                        sql_type = "NVARCHAR(100)"
                    elif string_lenght <= 255:
                        # Use NVARCHAR for string columns with a maximum length of 255
                        sql_type = "NVARCHAR(255)"
                    elif col_name in primary_keys:
                        raise ValueError(
                            f"Primary key {col_name} shouldn't be longer than 255 characters"
                        )
                    else:
                        sql_type = "NVARCHAR(MAX)"
                elif col_type == pl.Datetime:
                    if col_type.time_zone is not None:  # type: ignore
                        sql_type = "DATETIMEOFFSET(0)"
                    else:
                        sql_type = "DATETIME2(0)"
                elif col_type == pl.Decimal:
                    # Use DECIMAL with precision and scale
                    precision = col_type.precision  # type: ignore
                    scale = col_type.scale  # type: ignore
                    sql_type = f"DECIMAL({precision}, {scale})"
                else:
                    raise ValueError(f"Unsupported data type: {col_type}")

            # Add the column definition to the SQL script
            sql_script += f"    [{col_name}] {sql_type}"

            # Check if the column is a primary key
            if col_name in primary_keys:
                sql_script += " NOT NULL"

            sql_script += ",\n"

        # Remove the trailing comma and newline
        sql_script = sql_script[:-2] + "\n);\n"

        return sql_script

    def drop_table_sql_script(self, temp: bool = False) -> str:
        table_name = self.table_name if not temp else self.temp_table_name
        schema_table_relation = (
            self.schema_table_relation if not temp else self.schema_temp_table_relation
        )
        return textwrap.dedent(f"""
            IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = '{self.db_schema}' 
                AND TABLE_NAME = '{table_name}') 
                DROP TABLE {schema_table_relation};
            """)

    def drop_view_sql_script(self, temp: bool = False) -> str:
        table_name = self.table_name if not temp else self.temp_table_name
        schema_table_relation = (
            self.schema_table_relation if not temp else self.schema_temp_table_relation
        )
        return textwrap.dedent(f"""
            IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.VIEWS 
                WHERE TABLE_SCHEMA = '{self.db_schema}' 
                AND TABLE_NAME = '{table_name}') 
                DROP VIEW {schema_table_relation};
            """)

    def primary_key_table_sql_script(self, temp: bool = False) -> str:
        schema_table_relation = (
            self.schema_table_relation if not temp else self.schema_temp_table_relation
        )
        pk = self.primary_keys
        tn = self.table_name
        if len(pk) == 0:
            raise ValueError("No primary keys defined")
        # Add the nonclustered primary key with randon name
        dynamic_part = get_now_datetime().strftime("%Y%m%dT%H%M%S%f")
        sql_script = f"ALTER TABLE {schema_table_relation} ADD CONSTRAINT [pk_{tn}_{dynamic_part}] PRIMARY KEY NONCLUSTERED ({', '.join(pk)});\n"
        return sql_script

    def columnstore_table_sql_script(self, temp: bool = False) -> str:
        schema_table_relation = (
            self.schema_table_relation if not temp else self.schema_temp_table_relation
        )
        dynamic_part = get_now_datetime().strftime("%Y%m%dT%H%M%S%f")
        tn = self.table_name
        return f"CREATE CLUSTERED COLUMNSTORE INDEX [idx_{tn}_{dynamic_part}] ON {schema_table_relation};\n"

    def swap_table_with_temp(self) -> str:
        return textwrap.dedent(f"""
            -- Swap the tables
            BEGIN TRANSACTION;
            IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES t
                WHERE t.TABLE_NAME = '{self.table_name}' and t.TABLE_SCHEMA = '{self.db_schema}')
                EXEC sp_rename '{self.schema_table_relation}', '{self.table_name}_OLD';
            EXEC sp_rename '{self.schema_temp_table_relation}', '{self.table_name}';
            COMMIT TRANSACTION;

            -- Drop the old table
            DROP TABLE IF EXISTS [{self.db_schema}].[{self.table_name}_OLD];

            -- Drop the NEW temp table
            DROP TABLE IF EXISTS {self.schema_temp_table_relation};
        """)

    def bulk_insert_sql_script(
        self,
        file_path: str,
        temp: bool = False,
        codepage: str = "65001",  # UTF-8
        format: str = "CSV",
        first_row: int | None = None,
        tablock: bool = True,
        row_terminator: RowTerminator = RowTerminator.CRLF,
        field_terminator: str | None = None,
        field_quote: str | None = None,
        batch_size: int | None = None,
        rows_per_batch: int | None = None,
        max_errors: int = 0,
        order_columns: list[str] | None = None,
        format_file_path: str | None = None,
        error_file_path: str | None = None,
    ) -> str:
        schema_table_relation = (
            self.schema_table_relation if not temp else self.schema_temp_table_relation
        )
        """
        Generate a SQL script for BULK INSERT from a file.

        Args:
            file_path: Path to the source file
            codepage: Character encoding (default: 65001/UTF-8)
            format: File format (default: CSV)
            first_row: Starting row number (default: None)
            tablock: Whether to use table locking (default: True)
            row_terminator: Line ending character(s) (default: \r\n)
            field_terminator: Field separator character(s) (default: None)
            field_quote: Character used for quoting fields (default: None)
            batch_size: Size of each batch in bytes (default: None)
            rows_per_batch: Number of rows per batch (default: None)
            max_errors: Maximum allowed errors (default: 0 - no errors allowed)
            order_columns: List of columns for ordering (default: None)
            format_file_path: Path to format file (default: None)
            error_file_path: Path to error file (default: None)

        Returns:
            str: SQL BULK INSERT statement
        """
        options = []

        # Add basic options
        options.append(f"CODEPAGE = '{codepage}'")
        options.append(f"FORMAT = '{format}'")

        # Add conditional options
        if first_row is not None:
            options.append(f"FIRSTROW = {first_row}")

        if tablock:
            options.append("TABLOCK")

        if row_terminator is not None:
            options.append(f"ROWTERMINATOR = '{row_terminator}'")

        if field_terminator is not None:
            options.append(f"FIELDTERMINATOR = '{field_terminator}'")

        if field_quote is not None:
            options.append(f"FIELDQUOTE = '{field_quote}'")

        if batch_size is not None:
            options.append(f"BATCHSIZE = {batch_size}")

        if rows_per_batch is not None:
            options.append(f"ROWS_PER_BATCH = {rows_per_batch}")

        options.append(f"MAXERRORS = {max_errors}")

        if order_columns is not None and len(order_columns) > 0:
            options.append(f"ORDER ({', '.join(order_columns)})")

        if format_file_path is not None:
            options.append(f"FORMATFILE = '{format_file_path}'")

        if error_file_path is not None:
            options.append(f"ERRORFILE = '{error_file_path}'")

        # Format the options with proper indentation
        formatted_options = ",\n    ".join(options)

        # Construct the final SQL statement
        sql = textwrap.dedent(f"""BULK INSERT {schema_table_relation}
            FROM '{file_path}'
            WITH (
                {formatted_options}
            );""")

        return sql
