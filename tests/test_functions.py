from pathlib import Path
import pytest
import pandas as pd
import boto3
from moto import mock_aws
from mojap_metadata import Metadata

from scripts.utils import s3_path_join

from scripts.functions import (
    list_parquet_files_in_s3,
    read_parquet_file_to_dataframe,
    get_new_columns_definition,
    load_metadata,
    update_metadata,
    cast_column_to_type,
    cast_columns_to_correct_types,
    add_mojap_columns_to_dataframe,
    create_glue_database,
    )


@pytest.fixture
def sample_dataframe():
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "Source extraction date": [
                "2023-01-01",
                "2023-01-02",
                "2023-01-03",
            ],
        }
    )


@pytest.fixture
def sample_metadata():
    return Metadata(
        name="test_table",
        description="table containing information",
        file_format="parquet",
        columns=[
            {"name": "id", "type": "int64"},
            {"name": "name", "type": "string"},
            {
                "name": "Source extraction date",
                "type": "timestamp(ms)",
                "datetime_format": "%Y-%m-%d",
            },
        ],
    )


@mock_aws
def test_list_parquet_files_in_s3():
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="test-bucket")
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.put_object(
        Bucket="test-bucket",
        Key="test/file1.parquet",
        Body="",
    )
    s3.put_object(
        Bucket="test-bucket",
        Key="test/file2.parquet",
        Body="",
    )
    s3.put_object(
        Bucket="test-bucket", Key="test/file3.txt", Body=""
    )

    files = list_parquet_files_in_s3("test-bucket", "test/")
    assert len(files) == 2
    assert all(file.endswith(".parquet") for file in files)


def test_read_parquet_file_to_dataframe(tmp_path: Path):
    df = pd.DataFrame(
        {"col1": [1, 2, 3], "col2": ["a", "b", "c"]}
    )
    parquet_file = tmp_path / "test.parquet"
    df.to_parquet(parquet_file)

    result = read_parquet_file_to_dataframe(
        str(parquet_file)
    )
    pd.testing.assert_frame_equal(result, df)


def test_load_metadata(tmp_path: Path):
    metadata_content = '{"name": "test_table", "columns": [{"name": "id", "type": "int64"}]}' # noqa
    metadata_file = s3_path_join(
        tmp_path / "test_table.json"
    )
    metadata_file.write_text(metadata_content)

    result = load_metadata(str(tmp_path), "test_table")
    assert isinstance(result, Metadata)
    assert result.name == "test_table"
    assert len(result.columns) == 1


def test_update_metadata(sample_metadata: Metadata):
    updated_metadata = update_metadata(sample_metadata)
    new_column_names = [
        col["name"] for col in get_new_columns_definition()
    ]
    assert all(
        col in [c["name"] for c in updated_metadata.columns]
        for col in new_column_names
    )


def test_cast_column_to_type(sample_dataframe: pd.DataFrame):
    result = cast_column_to_type(
        sample_dataframe, "id", "int64"
    )
    assert result["id"].dtype == "int64"

    result = cast_column_to_type(
        result,
        "Source extraction date",
        "timestamp(ms)",
        "%Y-%m-%d",
    )
    assert pd.api.types.is_datetime64_any_dtype(
        result["Source extraction date"]
    )


def test_cast_columns_to_correct_types(
    sample_dataframe: pd.DataFrame, sample_metadata: Metadata
):
    result = cast_columns_to_correct_types(
        sample_dataframe, sample_metadata
    )
    assert result["id"].dtype == "int64"
    assert result["name"].dtype == "string"
    assert pd.api.types.is_datetime64_any_dtype(
        result["Source extraction date"]
    )


def test_add_mojap_columns_to_dataframe(sample_dataframe: pd.DataFrame):
    result = add_mojap_columns_to_dataframe(
        sample_dataframe, "v1.0", 1625097600
    )
    assert "mojap_start_datetime" in result.columns
    assert "mojap_image_tag" in result.columns
    assert "mojap_raw_filename" in result.columns
    assert "mojap_task_timestamp" in result.columns


@mock_aws
def test_create_glue_database():
    glue_client = boto3.client(
        "glue", region_name="eu-west-1"
    )
    db_dict = {
        "name": "test_db",
        "description": "Test database",
    }
    create_glue_database(glue_client, db_dict)

    # Check if the database was created
    response = glue_client.get_database(Name="test_db")
    assert response["Database"]["Name"] == "test_db"
    assert (
        response["Database"]["Description"]
        == "Test database"
    )


@mock_aws
def test_create_glue_database_already_exists():
    glue_client = boto3.client(
        "glue", region_name="eu-west-1"
    )
    db_dict = {
        "name": "test_db",
        "description": "Test database",
    }
    # Create the database first
    glue_client.create_database(
        DatabaseInput={"Name": "test_db"}
    )
    # Try to create it again
    create_glue_database(glue_client, db_dict)
