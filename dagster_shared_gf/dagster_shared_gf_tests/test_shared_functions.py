import pytest
from dagster import ScheduleDefinition, define_asset_job
import polars as pl
from datetime import datetime, timedelta, timezone
import dagster_shared_gf.shared_functions as sf
from dagster_shared_gf.shared_variables import UnresolvedAssetJobDefinition

# Example class and function definitions for testing

test_job = define_asset_job(name="dbt_dwh_sap_marts_job")

my_schedule = ScheduleDefinition(
    name="test",
    cron_schedule="14 1 * * *",  # 10:01 AM every day
    execution_timezone="America/Tegucigalpa",
    job=test_job,
)

EXPECTED_SQL_CREATE_TABLE = """CREATE TABLE [dbo].[people] (
    [id] INT NOT NULL,
    [name] NVARCHAR(50) NOT NULL,
    [big_age] BIGINT,
    [dob] DATE,
    [created_at] DATETIME2(0),
    [datetime_timezone] DATETIMEOFFSET(0),
    [is_active] BIT,
    [float] FLOAT(53),
    [decimal] DECIMAL(10, 5),
    [string_non_primary_key] NVARCHAR(100),
    [101_string] NVARCHAR(255),
    [256_string] NVARCHAR(MAX)
);
"""

EXPECTED_SQL_CREATE_TABLE_TEMP_TABLE_NAME = """CREATE TABLE [dbo].[people_TEMP] (
    [id] INT NOT NULL
);
"""

def my_schedule_function():
    pass


def another_function():
    pass


another_variable = 0

# print(test_job)
# Filter instances of ScheduleDefinition
# print(get_all_instances_of_class([ScheduleDefinition]))

# Filter variables created by the function define_asset_job
# print(get_variables_created_by_function(define_asset_job))


# Test get_all_instances_of_class
def test_get_all_instances_of_class():
    instances = sf.get_all_instances_of_class([ScheduleDefinition])
    assert len(instances) == 1, f"Expected 1 instances, got {len(instances)}"
    assert my_schedule in instances, f"Expected 1 instances, got {len(instances)}"


# Test get_variables_created_by_function
def test_get_variables_created_by_function():
    variables = sf.get_all_instances_of_class(
        class_type_list=[UnresolvedAssetJobDefinition]
    )
    assert len(variables) == 1, f"Expected 1 variables, got {len(variables)}"
    assert test_job in variables, "Expected test_job to be in variables"


class MockAssetsDefinition:
    def __init__(self, keys, tags_by_key):
        self.keys = keys
        self.tags_by_key = tags_by_key


@pytest.fixture
def assets_definitions():
    return [
        MockAssetsDefinition(
            keys=["key1"], tags_by_key={"key1": {"tag1": "value1", "tag2": "value2"}}
        ),
        MockAssetsDefinition(keys=["key2"], tags_by_key={"key2": {"tag1": "value1"}}),
        MockAssetsDefinition(keys=["key3"], tags_by_key={"key3": {"tag3": "value3"}}),
        MockAssetsDefinition(keys=["key4"], tags_by_key={"key4": {"tag2": "value2"}}),
    ]


@pytest.fixture
def patch_isinstance(mocker):
    mocker.patch(
        "dagster_shared_gf.shared_functions.isinstance",
        side_effect=lambda obj, cls: isinstance(obj, MockAssetsDefinition)
        if cls.__name__ == "AssetsDefinition"
        else isinstance(obj, cls),
    )


def test_all_tags_match(patch_isinstance, assets_definitions):
    tags_to_match = {"tag1": "value1"}
    filtered = sf.filter_assets_by_tags(
        assets_definitions, tags_to_match, "all_tags_match"
    )
    assert len(filtered) == 2


def test_any_tag_matches(patch_isinstance, assets_definitions):
    tags_to_match = {"tag1": "value1"}
    filtered = sf.filter_assets_by_tags(
        assets_definitions, tags_to_match, "any_tag_matches"
    )
    assert len(filtered) == 2


def test_exclude_if_all_tags(patch_isinstance, assets_definitions):
    tags_to_match = {"tag1": "value1"}
    filtered = sf.filter_assets_by_tags(
        assets_definitions, tags_to_match, "exclude_if_all_tags"
    )
    assert len(filtered) == 2


def test_exclude_if_any_tag(patch_isinstance, assets_definitions):
    tags_to_match = {"tag1": "value1"}
    filtered = sf.filter_assets_by_tags(
        assets_definitions, tags_to_match, "exclude_if_any_tag"
    )
    assert len(filtered) == 2


def test_no_matching_tags(patch_isinstance, assets_definitions):
    tags_to_match = {"tag4": "value4"}
    filtered = sf.filter_assets_by_tags(
        assets_definitions, tags_to_match, "all_tags_match"
    )
    assert len(filtered) == 0


def test_empty_assets_list(patch_isinstance):
    tags_to_match = {"tag1": "value1"}
    filtered = sf.filter_assets_by_tags([], tags_to_match, "all_tags_match")
    assert len(filtered) == 0


class TestSQLScriptGenerator:
    @pytest.fixture(autouse=True)
    def patch_getnow(self, mocker):
        path = sf.get_function_path(sf.get_now_datetime)
        mocker.patch(
            path,
            return_value=datetime(2022, 1, 1, 12, 0, 0),
        )

    def test_create_table_sql_script(self):
        # Example usage and test
        df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["John", "Jane", "Bob"],
                "big_age": [25, 30, 35],
                "dob": [
                    datetime(1990, 1, 1),
                    datetime(1995, 1, 1),
                    datetime(2000, 1, 1),
                ],
                "created_at": [
                    datetime(2022, 1, 1, 12, 0, 0),
                    datetime(2022, 1, 2, 12, 0, 0),
                    datetime(2022, 1, 3, 12, 0, 0),
                ],
                "datetime_timezone": [
                    datetime(
                        2022, 1, 1, 12, 0, 0, tzinfo=timezone(timedelta(hours=-5))
                    ),
                    datetime(
                        2022, 1, 1, 12, 0, 0, tzinfo=timezone(timedelta(hours=-8))
                    ),
                    datetime(2022, 1, 1, 12, 0, 0, tzinfo=timezone(timedelta(hours=1))),
                ],
                "is_active": [True, False, True],
                "float": [100.0, 200.0, 300.0],
                "decimal": [1000.2322, 2001.12312, 3001.01],
                "string_non_primary_key": ["a", None, "c"],
                "101_string": ["1" * 101, None, "c"],
                "256_string": ["1" * 256, None, "c"],
            },
            schema={
                "id": pl.Int32,
                "name": pl.Utf8,
                "big_age": pl.Int64,
                "dob": pl.Date,
                "created_at": pl.Datetime,
                "datetime_timezone": pl.Datetime,
                "is_active": pl.Boolean,
                "float": pl.Float64,
                "decimal": pl.Decimal(10, 5),
                "string_non_primary_key": pl.Utf8,
                "101_string": pl.Utf8,
                "256_string": pl.Utf8,
            },
        )

        sg = sf.SQLScriptGenerator(
            df=df, db_schema="dbo", table_name="people", primary_keys=("id", "name")
        )
        generated_script = sg.create_table_sql_script()
        primary_key = sg.primary_key_table_sql_script()
        columnstore = sg.columnstore_table_sql_script()
        print(df.schema.items())

        assert (
            generated_script == EXPECTED_SQL_CREATE_TABLE
        ), f"The generated SQL script:\n {generated_script} does not match the expected output."
        assert (
            primary_key
            == "ALTER TABLE [dbo].[people] ADD CONSTRAINT [pk_people_20220101T120000000000] PRIMARY KEY NONCLUSTERED (id, name);\n"
        ), f"The generated SQL script:\n {primary_key} does not match the expected output."
        assert (
            columnstore
            == "CREATE CLUSTERED COLUMNSTORE INDEX [idx_people_20220101T120000000000] ON [dbo].[people];\n"
        ), f"The generated SQL script:\n {columnstore} does not match the expected output."

    def test_raises_error(self):
        with pytest.raises(ValueError):
            sg = sf.SQLScriptGenerator(
                df=pl.DataFrame({"id": [1]}),
                db_schema="dbo",
                table_name="people",
                primary_keys=("id", "name"),
            )
            sg.create_table_sql_script()

        with pytest.raises(ValueError):
            sg = sf.SQLScriptGenerator(
                df=pl.DataFrame({"id": [1]}),
                db_schema="dbo",
                table_name="people",
                primary_keys=("id", "name", "age"),
            )
            sg.primary_key_table_sql_script()

    def test_temp_table_name(self):
        sg = sf.SQLScriptGenerator(
            df=pl.DataFrame({"id": [1]}, schema={"id": pl.Int32}),
            db_schema="dbo",
            table_name="people",
            primary_keys=("id",),
            temp_table_name="people_TEMP",
        )
        generated_script = sg.create_table_sql_script()
        print(generated_script)
        assert (
            generated_script
            == EXPECTED_SQL_CREATE_TABLE_TEMP_TABLE_NAME
        ), f"The generated SQL script:\n {generated_script} does not match the expected output."
        assert (
            sg.primary_key_table_sql_script()
            == "ALTER TABLE [dbo].[people_TEMP] ADD CONSTRAINT [pk_people_20220101T120000000000] PRIMARY KEY NONCLUSTERED (id);\n"	
        )
        assert (
            sg.columnstore_table_sql_script()
            == "CREATE CLUSTERED COLUMNSTORE INDEX [idx_people_20220101T120000000000] ON [dbo].[people_TEMP];\n"
        )


print("All tests passed!")

if __name__ == "__main__":
    TSQL = TestSQLScriptGenerator()
    TSQL.test_create_table_sql_script()
