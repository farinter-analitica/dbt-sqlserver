from io import BytesIO
import polars as pl
import os
import re
def test_read_excel_with_calamine():
    # Define the path to the Excel file
    excel_file_path = os.path.join(os.path.dirname(__file__), 'test_excel_file.xlsx')
    dfd: dict[str, pl.DataFrame]

    with open(excel_file_path, "rb") as f:
        # Read the Excel file using Polars with Calamine
        dfd = pl.read_excel(
            BytesIO(f.read()),
            sheet_id=0,
            # , sheet_name='carga',
            # , columns=list(schema_config.expected_columns.values())
            engine="calamine",
            raise_if_empty=False,
        )
        sheet_name_pattern = re.compile(r"\bcarga.*", re.IGNORECASE)

    # Filtering sheets whose names match the pattern
    for sheet_name in dfd.keys():
        if sheet_name_pattern.match(sheet_name):
            df = dfd[sheet_name]
            break
    else:
        raise ValueError("No sheet found matching the pattern")

    # Perform assertions to verify the data
    assert df is not None, "DataFrame should not be None"
    assert not df.is_empty(), "DataFrame should not be empty"
    assert "A" in df.columns, "DataFrame should contain 'A' column"
    assert "B" in df.columns, "DataFrame should contain 'B' column"

    # Print the DataFrame for visual inspection (optional)
    #print(df)

# Run the test
if __name__ == "__main__":
    test_read_excel_with_calamine()