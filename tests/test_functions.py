import pandas as pd
from scripts.functions import (
    cast_column_to_type,
    add_mojap_start_datetime_column,
    get_new_columns_definition
)

# Test case for cast_column_to_type
def test_cast_column_to_type():
    df = pd.DataFrame({"col1": ["2023-10-27", "2023-10-28"], "col2": [1, 2]})
    df = cast_column_to_type(df, "col1", "timestamp(ms)", "%Y-%m-%d")
    assert str(df["col1"].dtype) == "datetime64[ns]"

# Test case for add_mojap_start_datetime_column
def test_add_mojap_start_datetime_column():
    df = pd.DataFrame({"Source extraction date": ["2023-10-27", "2023-10-28"]})
    df = add_mojap_start_datetime_column(df)
    assert "mojap_start_datetime" in df.columns

# Test case for get_new_columns_definition
def test_get_new_columns_definition():
    new_columns = get_new_columns_definition()
    new_columns = [items["name"] for items in new_columns]
    assert isinstance(new_columns, list), "The function should return a list."
    assert len(new_columns) == 4, "The function should return four column definitions."
