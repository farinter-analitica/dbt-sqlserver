import polars as pl

def generate_table_script(table_name: str, df: pl.DataFrame, primary_keys: list[str] = []) -> str:
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
    # Get the dict with col_name: lenght of string type columns
    string_lengths = {col_name: max(df[col_name].str.lengths()) for col_name, data_type in df.schema if data_type == pl.Utf8}
    # Get the maximum length of string columns
    print(schema)
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
            if col_name in primary_keys and not string_lengths[col_name] > 50:
                sql_type = "NVARCHAR(50) NOT NULL"
            elif string_lengths[col_name] <= 100:
                sql_type = "NVARCHAR(100)"
            elif string_lengths[col_name] <= 255:
            # Use NVARCHAR for string columns with a maximum length of 255
                sql_type = "NVARCHAR(255)"
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
            sql_script += " NOT NULL PRIMARY KEY"

        sql_script += ",\n"

    # Remove the trailing comma and newline
    sql_script = sql_script[:-2] + "\n);\n"

    return sql_script

# Example usage
df = pl.DataFrame({
    "id": [1, 2, 3],
    "name": ["John", "Jane", "Bob"],
    "big_age": [25, 30, 35],
    "dob": ["1990-01-01", "1995-01-01", "2000-01-01"],
    "created_at": ["2022-01-01 12:00:00", "2022-01-02 12:00:00", "2022-01-03 12:00:00"],
    "datetime_timezone" : ["2022-01-01 12:00:00+02:00", "2022-01-02 12:00:00+01:00", "2022-01-03 12:00:00-03:00"],
    "is_active": [True, False, True],
    "float": [100.0, 200.0, 300.0],
    "decimal": [1000.2322, 2001.12312, 3001.01],
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
})

expected_output = \
"""CREATE TABLE people (
    id INT NOT NULL PRIMARY KEY,
    name NVARCHAR(50) NOT NULL,
    big_age BIGINT,
    dob DATE,
    created_at DATETIME2(0),
    datetime_timezone DATETIMEOFFSET(0),
    is_active BIT,
    float FLOAT(53),
    decimal DECIMAL(10, 5)
);
"""
generated_script = generate_table_script("people", df, primary_keys=["id", "name"])

print(generated_script)

assert generated_script == expected_output, "The generated SQL script does not match the expected output."