import polars as pl
from datetime import timezone, timedelta, datetime

def get_now() -> datetime:
    return datetime.now()

def create_table_sql_script(table_name: str, df: pl.DataFrame, primary_keys: tuple[str, ...] = tuple()) -> str:
    """
    Generate a SQL script to create a table based on a Polars DataFrame schema.

    Args:
    - table_name (str): The name of the table.
    - df (pl.DataFrame): The Polars DataFrame.
    - primary_keys (list[str], optional): A list of column names to be used as primary keys. Defaults to [].

    Returns:
    - str: The SQL script to create the table.
    """

    # Get the schema of the DataFrame
    schema = df.collect_schema()
    # Check correct primary keys
    for pk in primary_keys:
        if pk not in schema:
            raise ValueError(f"Primary key {pk} not in schema, available keys: {schema.keys()}")
        # Validate not nulls
        if df.get_column(pk).null_count() > 0:
            raise ValueError(f"Primary key {pk} cannot be null")

    # Initialize the SQL script
    sql_script = f"CREATE TABLE {table_name} (\n"

    # Iterate over the columns in the schema
    for col_name, col_type in schema.items():
        
        # Map the Polars data type to a SQL Server data type
        if col_type == pl.Int8:
            sql_type = "TINYINT"
        elif col_type == pl.Int16:
            sql_type = "SMALLINT"
        elif col_type == pl.Int32:
            sql_type = "INT"
        elif col_type == pl.Int64:
            sql_type = "BIGINT"
        elif col_type == pl.Float32:
            sql_type = "FLOAT(24)"
        elif col_type == pl.Float64:
            sql_type = "FLOAT(53)"
        elif col_type == pl.Utf8:
            string_lenght = df.get_column(col_name).str.len_chars().max()

            if col_name in primary_keys and not string_lenght > 50:
                sql_type = "NVARCHAR(50)"
            elif string_lenght <= 100:
                sql_type = "NVARCHAR(100)"
            elif string_lenght <= 255:
            # Use NVARCHAR for string columns with a maximum length of 255
                sql_type = "NVARCHAR(255)"
            elif col_name in primary_keys:
                raise ValueError(f"Primary key {col_name} shouldn't be longer than 255 characters")
            else:
                sql_type = "NVARCHAR(MAX)"
        elif col_type == pl.Boolean:
            sql_type = "BIT"
        elif col_type == pl.Datetime:
            if col_type.time_zone is not None:
                sql_type = "DATETIMEOFFSET(0)"
            else:
                sql_type = "DATETIME2(0)"
        elif col_type == pl.Date:
            sql_type = "DATE"
        elif col_type == pl.Decimal:
            # Use DECIMAL with precision and scale
            precision = col_type.precision
            scale = col_type.scale
            sql_type = f"DECIMAL({precision}, {scale})"
        else:
            raise ValueError(f"Unsupported data type: {col_type}")


        # Add the column definition to the SQL script
        sql_script += f"    {col_name} {sql_type}"

        # Check if the column is a primary key
        if col_name in primary_keys:
            sql_script += " NOT NULL"

        sql_script += ",\n"

    # Remove the trailing comma and newline
    sql_script = sql_script[:-2] + "\n);\n"

    return sql_script

def primary_key_table_sql_script(table_name: str, primary_keys: tuple[str, ...]) -> str:
    # Add the nonclustered primary key with randon name
    dynamic_part = get_now().strftime("%Y%m%dT%H%M%S%f")
    if len(primary_keys) > 0:
        sql_script = f"ALTER TABLE {table_name} ADD CONSTRAINT pk_{table_name}_{dynamic_part} NONCLUSTERED PRIMARY KEY ({', '.join(primary_keys)});\n"

    return sql_script

def columnstore_table_sql_script(table_name: str) -> str:
    dynamic_part = get_now().strftime("%Y%m%dT%H%M%S%f")

    return f"CREATE CLUSTERED COLUMNSTORE INDEX idx_{table_name}_{dynamic_part} ON {table_name};\n"

# Example usage and test
df = pl.DataFrame({
    "id": [1, 2, 3],
    "name": ["John", "Jane", "Bob"],
    "big_age": [25, 30, 35],
    "dob": [datetime(1990, 1, 1), datetime(1995, 1, 1), datetime(2000, 1, 1)],
    "created_at": [datetime(2022, 1, 1, 12, 0, 0), 
                   datetime(2022, 1, 2, 12, 0, 0), 
                   datetime(2022, 1, 3, 12, 0, 0)],
    "datetime_timezone": [datetime(2022, 1, 1, 12, 0, 0, tzinfo=timezone(timedelta(hours=-5))), 
                          datetime(2022, 1, 1, 12, 0, 0, tzinfo=timezone(timedelta(hours=-8))), 
                          datetime(2022, 1, 1, 12, 0, 0, tzinfo=timezone(timedelta(hours=1)))],
    "is_active": [True, False, True],
    "float": [100.0, 200.0, 300.0],
    "decimal": [1000.2322, 2001.12312, 3001.01],
    "string_non_primary_key": ["a", None, "c"],
    "101_string": ["1"*101, None, "c"],
    "256_string": ["1"*256, None, "c"],
}, schema={
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
})

expected_output = \
"""CREATE TABLE people (
    id INT NOT NULL,
    name NVARCHAR(50) NOT NULL,
    big_age BIGINT,
    dob DATE,
    created_at DATETIME2(0),
    datetime_timezone DATETIMEOFFSET(0),
    is_active BIT,
    float FLOAT(53),
    decimal DECIMAL(10, 5),
    string_non_primary_key NVARCHAR(100),
    101_string NVARCHAR(255),
    256_string NVARCHAR(MAX)
);
"""

if __name__ == "__main__":
    from pytest import fixture

    @fixture
    def patch_getnow(mocker):
        mocker.patch('dagster_shared_gf.shared_functions.get_now', return_value=datetime(2022, 1, 1, 12, 0, 0))

    def test_create_table_sql_script(patch_getnow):
        generated_script = create_table_sql_script(table_name="people",  df=df, primary_keys=("id", "name"))
        primary_key = primary_key_table_sql_script(table_name="people", primary_keys=("id", "name"))
        columnstore = columnstore_table_sql_script(table_name="people")
        print (df.schema.items())

        assert generated_script == expected_output, f"The generated SQL script:\n {generated_script} does not match the expected output."
        assert primary_key == "ALTER TABLE people ADD CONSTRAINT pk_people_20220101T120000 NONCLUSTERED PRIMARY KEY (id, name);\n", f"The generated SQL script:\n {primary_key} does not match the expected output."
        assert columnstore == "CREATE CLUSTERED COLUMNSTORE INDEX idx_people_20220101T120000 ON people;\n",    f"The generated SQL script:\n {columnstore} does not match the expected output."