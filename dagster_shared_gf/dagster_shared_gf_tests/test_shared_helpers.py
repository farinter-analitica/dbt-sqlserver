import datetime as dt

import polars as pl
import pytest

from dagster_shared_gf.shared_helpers import RowTerminator, DataframeSQLScriptGenerator


class DummyDatetime:
    def strftime(self, fmt):
        return "20240101T120000000000"


def patch_get_now_datetime(monkeypatch):
    import dagster_shared_gf.shared_helpers as helpers

    monkeypatch.setattr(helpers, "get_now_datetime", lambda: DummyDatetime())


class TestSQLScriptGenerator:
    def test_sqlscriptgenerator_basic_properties(self):
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        gen = DataframeSQLScriptGenerator(df, db_schema="dbo", table_name="mytable")
        assert gen.df.equals(df)
        assert gen.db_schema == "dbo"
        assert gen.table_name == "mytable"
        assert gen.temp_table_name == "mytable"
        assert gen.schema_table_relation == "[dbo].[mytable]"
        assert gen.schema_temp_table_relation == "[dbo].[mytable]"
        assert gen.primary_keys == tuple()
        with pytest.raises(ValueError):
            _ = gen.db_name

    def test_sqlscriptgenerator_full_relation(self):
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        gen = DataframeSQLScriptGenerator(
            df, db_schema="dbo", table_name="mytable", db_name="testdb"
        )
        assert gen.full_relation == "[testdb].[dbo].[mytable]"

    def test_sqlscriptgenerator_validate_and_format_pks_success(self):
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        gen = DataframeSQLScriptGenerator(
            df, db_schema="dbo", table_name="mytable", primary_keys=("id",)
        )
        assert gen.formatted_primary_keys == ("[id]",)

    def test_sqlscriptgenerator_validate_and_format_pks_duplicate(self):
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        with pytest.raises(ValueError, match="Duplicate primary key: id"):
            DataframeSQLScriptGenerator(
                df, db_schema="dbo", table_name="mytable", primary_keys=("id", "id")
            )

    def test_sqlscriptgenerator_validate_and_format_pks_not_in_schema(self):
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        with pytest.raises(ValueError, match="Primary key not_in_schema not in schema"):
            DataframeSQLScriptGenerator(
                df,
                db_schema="dbo",
                table_name="mytable",
                primary_keys=("not_in_schema",),
            )

    def test_sqlscriptgenerator_validate_and_format_pks_nulls(self):
        df = pl.DataFrame({"id": [1, None], "name": ["a", "b"]})
        with pytest.raises(ValueError, match="Primary key id cannot be null"):
            DataframeSQLScriptGenerator(
                df, db_schema="dbo", table_name="mytable", primary_keys=("id",)
            )

    def test_sqlscriptgenerator_validate_and_format_pks_duplicates_in_data(self):
        df = pl.DataFrame({"id": [1, 1], "name": ["a", "b"]})
        with pytest.raises(ValueError, match="cannot have duplicates"):
            DataframeSQLScriptGenerator(
                df, db_schema="dbo", table_name="mytable", primary_keys=("id",)
            )

    def test_create_table_sql_script_types(self):
        df = pl.DataFrame(
            {
                "id": [1, 2],
                "val_float": [1.1, 2.2],
                "val_bool": [True, False],
                "val_date": [
                    dt.datetime(2023, 1, 1).date(),
                    dt.datetime(2023, 1, 2).date(),
                ],
                "val_datetime": [dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 2)],
                "val_str": ["abc", "def"],
            }
        )
        gen = DataframeSQLScriptGenerator(
            df, db_schema="dbo", table_name="t", primary_keys=("id",)
        )
        sql = gen.create_table_sql_script()
        assert "TINYINT" not in sql
        assert "[id] INT NOT NULL" in sql or "[id] BIGINT NOT NULL" in sql
        assert "[val_float] FLOAT(24)" in sql or "[val_float] FLOAT(53)" in sql
        assert "[val_bool] BIT" in sql
        assert "[val_date] DATE" in sql
        assert "[val_datetime] DATETIME" in sql
        assert (
            "[val_str] NVARCHAR(100)" in sql
            or "[val_str] NVARCHAR(255)" in sql
            or "[val_str] NVARCHAR(MAX)" in sql
        )

    def test_create_table_sql_script_string_length(self):
        df = pl.DataFrame(
            {"id": [1, 2], "short_str": ["a", "b"], "long_str": ["x" * 200, "y" * 200]}
        )
        gen = DataframeSQLScriptGenerator(
            df, db_schema="dbo", table_name="t", primary_keys=("id",)
        )
        sql = gen.create_table_sql_script()
        assert (
            "[short_str] NVARCHAR(100)" in sql
            or "[short_str] NVARCHAR(255)" in sql
            or "[short_str] NVARCHAR(MAX)" in sql
        )
        assert "[long_str] NVARCHAR(255)" in sql or "[long_str] NVARCHAR(MAX)" in sql

    def test_create_table_sql_script_primary_key_too_long(self):
        df = pl.DataFrame({"id": [1, 2], "pk_str": ["x" * 300, "y" * 300]})
        with pytest.raises(
            ValueError, match="have string values longer than 255 characters."
        ):
            DataframeSQLScriptGenerator(
                df, db_schema="dbo", table_name="t", primary_keys=("pk_str",)
            )

    def test_create_table_sql_script_unsupported_type(self):
        class DummyType:
            pass

        df = pl.DataFrame({"id": [1, 2]})
        gen = DataframeSQLScriptGenerator(df, db_schema="dbo", table_name="t")
        gen._df_schema = pl.Schema({"id": pl.DataType()})
        with pytest.raises(ValueError, match="Unsupported data type"):
            gen.create_table_sql_script()

    def test_drop_table_sql_script_and_view_sql_script(self):
        df = pl.DataFrame({"id": [1, 2]})
        gen = DataframeSQLScriptGenerator(df, db_schema="dbo", table_name="t")
        sql = gen.drop_table_sql_script()
        assert "DROP TABLE" in sql
        sqlv = gen.drop_view_sql_script()
        assert "DROP VIEW" in sqlv

    def test_primary_key_table_sql_script(self, monkeypatch):
        df = pl.DataFrame({"id": [1, 2]})
        gen = DataframeSQLScriptGenerator(
            df, db_schema="dbo", table_name="t", primary_keys=("id",)
        )
        patch_get_now_datetime(monkeypatch)
        sql = gen.primary_key_table_sql_script()
        assert "ALTER TABLE" in sql and "PRIMARY KEY" in sql

    def test_primary_key_table_sql_script_no_pk(self):
        df = pl.DataFrame({"id": [1, 2]})
        gen = DataframeSQLScriptGenerator(df, db_schema="dbo", table_name="t")
        with pytest.raises(ValueError, match="No primary keys defined"):
            gen.primary_key_table_sql_script()

    def test_columnstore_table_sql_script(self, monkeypatch):
        df = pl.DataFrame({"id": [1, 2]})
        gen = DataframeSQLScriptGenerator(df, db_schema="dbo", table_name="t")
        patch_get_now_datetime(monkeypatch)
        sql = gen.columnstore_table_sql_script()
        assert "CREATE CLUSTERED COLUMNSTORE INDEX" in sql

    def test_swap_table_with_temp(self):
        df = pl.DataFrame({"id": [1, 2]})
        gen = DataframeSQLScriptGenerator(df, db_schema="dbo", table_name="t")
        sql = gen.swap_table_with_temp()
        assert "sp_rename" in sql and "DROP TABLE IF EXISTS" in sql

    def test_bulk_insert_sql_script_basic(self):
        df = pl.DataFrame({"id": [1, 2]})
        gen = DataframeSQLScriptGenerator(df, db_schema="dbo", table_name="t")
        sql = gen.bulk_insert_sql_script(
            file_path="file.csv",
            codepage="65001",
            format="CSV",
            first_row=2,
            tablock=True,
            row_terminator=RowTerminator.LF,
            field_terminator=",",
            field_quote='"',
            batch_size=1000,
            rows_per_batch=500,
            max_errors=1,
            order_columns=["id"],
            format_file_path="fmt.fmt",
            error_file_path="err.txt",
        )
        assert "BULK INSERT" in sql
        assert "FROM 'file.csv'" in sql
        assert "CODEPAGE = '65001'" in sql
        assert "FORMAT = 'CSV'" in sql
        assert "FIRSTROW = 2" in sql
        assert "TABLOCK" in sql
        assert "ROWTERMINATOR = '\\n'" in sql
        assert "FIELDTERMINATOR = ','" in sql
        assert "FIELDQUOTE = '\"'" in sql
        assert "BATCHSIZE = 1000" in sql
        assert "ROWS_PER_BATCH = 500" in sql
        assert "MAXERRORS = 1" in sql
        assert "ORDER (id)" in sql
        assert "FORMATFILE = 'fmt.fmt'" in sql
        assert "ERRORFILE = 'err.txt'" in sql

    def test_bulk_insert_sql_script_minimal(self):
        df = pl.DataFrame({"id": [1, 2]})
        gen = DataframeSQLScriptGenerator(df, db_schema="dbo", table_name="t")
        sql = gen.bulk_insert_sql_script(file_path="file.csv")
        assert "BULK INSERT" in sql
        assert "FROM 'file.csv'" in sql
        assert "CODEPAGE = '65001'" in sql
        assert "FORMAT = 'CSV'" in sql
        assert "TABLOCK" in sql
        assert "MAXERRORS = 0" in sql

    def test_clean_dataframe_for_sql_nan_inf_rounding(self):
        df = pl.DataFrame(
            {
                "a": [1.123456789, float("inf"), float("nan"), -float("inf")],
                "b": [1, 2, 3, 4],
                "c": ["a", "b", "c", "d"],
            }
        )
        gen = DataframeSQLScriptGenerator(df, db_schema="dbo", table_name="t")
        cleaned = gen.clean_dataframe_for_sql(df, rounding=2)
        a_col = cleaned["a"].to_list()
        assert a_col[1] is None
        assert a_col[2] is None
        assert a_col[3] is None
        assert all(isinstance(x, float) or x is None for x in a_col)
        assert all(isinstance(x, int) for x in cleaned["b"].to_list())
        assert all(
            len(str(x).split(".")[1]) <= 2 if x is not None else True
            for x in a_col
            if isinstance(x, float)
        )
