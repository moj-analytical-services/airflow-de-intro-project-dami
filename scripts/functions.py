import os
import time
import logging
from typing import Dict, Optional, Union
import awswrangler as wr
import boto3
import pandas as pd
import pyarrow.parquet as pq
from arrow_pd_parser import writer
from botocore.exceptions import BotoCoreError, ClientError
from dataengineeringutils3.s3 import (
    get_filepaths_from_s3_folder
)
from mojap_metadata import Metadata
from mojap_metadata.converters.glue_converter import (
    GlueConverter,
)


def extract_data_to_s3(s3_path: str, local_base_path: str):
    """Extract data from local directory to S3."""
    for root, _, files in os.walk(local_base_path):
        for file in files:
            if file.endswith(".parquet"):
                file_path = os.path.join(root, file)
                s3_file_path = os.path.join(s3_path, file)
                try:
                    wr.s3.upload(file_path, s3_file_path)
                    logging.info(f"Uploaded {file} to {s3_path}")
                except Exception as e:
                    logging.info(
                        f"Failed to upload {file} to {s3_path}. Error: {str(e)}" # noqa
                    )
    logging.info("Extraction complete")


def create_s3_client():
    """Creates and returns a boto3 S3 resource."""
    return boto3.resource("s3")


def list_parquet_files_in_s3(
    bucket_name: str,
    prefix: str,
    partitions: Optional[Dict[str, str]] = None,
) -> list:
    """Lists Parquet files in an S3 path with optional partition filtering."""
    s3 = create_s3_client()
    bucket = s3.Bucket(bucket_name)
    parquet_files = []
    for obj in bucket.objects.filter(Prefix=prefix):
        if obj.key.endswith(".parquet"):
            if partitions and not any(partition in obj.key for partition in partitions):  # noqa
                continue
            parquet_files.append(f"s3://{bucket_name}/{obj.key}")
    return parquet_files


def read_parquet_file_to_dataframe(
    parquet_file: str,
) -> pd.DataFrame:
    """Reads a single Parquet file into a Pandas DataFrame."""
    return pq.ParquetDataset(parquet_file).read().to_pandas()


def load_data_from_s3(
    s3_path: str, partitions: Optional[Dict[str, str]] = None
) -> pd.DataFrame:
    """Loads and concatenates Parquet data from S3."""
    bucket_name, prefix = s3_path.replace("s3://", "").split("/", 1)
    parquet_files = list_parquet_files_in_s3(bucket_name, prefix, partitions)
    dfs = [read_parquet_file_to_dataframe(file) for file in parquet_files]
    full_df = pd.concat(dfs, ignore_index=True)
    logging.info("Loading data from landing bucket")
    return full_df


def load_metadata(metadata_folder: str, table_name: str) -> Metadata:
    """Load metadata from S3."""
    metadata_path = os.path.join(
        metadata_folder,
        f"{table_name}.json"
        )
    metadata = Metadata.from_json(metadata_path)
    metadata.name = table_name
    return metadata


def get_new_columns_definition() -> list:
    """Returns a list of new column definitions."""
    return [
        {
            "name": "mojap_start_datetime",
            "type": "timestamp(ms)",
            "datetime_format": "%Y-%m-%dT%H:%M:%S",
            "description": "extraction start date",
        },
        {
            "name": "mojap_image_tag",
            "type": "string",
            "description": "image version",
        },
        {
            "name": "mojap_raw_filename",
            "type": "string",
            "description": "",
        },
        {
            "name": "mojap_task_timestamp",
            "type": "timestamp(ms)",
            "datetime_format": "%Y-%m-%dT%H:%M:%S",
            "description": "",
        },
    ]


def update_metadata(metadata: Metadata) -> Metadata:
    """Updates metadata with new columns."""
    new_columns = get_new_columns_definition()
    for new_column in new_columns:
        metadata.update_column(new_column)
    return metadata


def cast_column_to_type(
    df: pd.DataFrame,
    column_name: str,
    column_type: str,
    datetime_format: Optional[str] = None,
) -> pd.DataFrame:
    """Casts a single column in a DataFrame to a specific type."""
    if column_name not in df.columns:
        df[column_name] = (
            pd.NaT
            if column_type == "timestamp(ms)"
            else ""
            if column_type == "string"
            else pd.NA
        )

    if column_type == "timestamp(ms)":
        df[column_name] = pd.to_datetime(
            df[column_name],
            format=datetime_format or "%Y-%m-%dT%H:%M:%S",
        )
    else:
        df[column_name] = df[column_name].astype(column_type)
    return df


def cast_columns_to_correct_types(df: pd.DataFrame, metadata: Metadata) -> pd.DataFrame:  # noqa
    """Casts columns in a DataFrame to types defined in metadata."""
    for column in metadata.columns:
        df = cast_column_to_type(
            df,
            column["name"],
            column["type"],
            column.get("datetime_format"),
        )
    return df


def add_mojap_columns_to_dataframe(
    df: pd.DataFrame,
    mojap_image_version: str,
    mojap_extraction_ts: int,
) -> pd.DataFrame:
    """Adds all MOJAP-specific columns to the DataFrame."""
    df["mojap_start_datetime"] = pd.to_datetime(df["Source extraction date"])
    df["mojap_image_tag"] = mojap_image_version
    df["mojap_raw_filename"] = "people-100000.csv"
    df["mojap_task_timestamp"] = pd.to_datetime(mojap_extraction_ts, unit="s")
    return df


def create_glue_database(
    glue_client, db_dict: Dict[str, Union[str, None]]
) -> None:
    """Creates a Glue database if it doesn't exist."""
    try:
        glue_client.get_database(Name=db_dict["name"])
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityNotFoundException":
            db_meta = {
                "DatabaseInput": {
                    "Name": db_dict["name"],
                    "Description": db_dict["description"],
                }
            }
            try:
                glue_client.create_database(**db_meta)
                logging.info(f"Created Glue database '{db_dict['name']}'")
            except Exception as create_error:
                logging.info(
                    f"Failed to create database '{db_dict['name']}'. \
                        Error: {str(create_error)}"
                )
        else:
            logging.info(
                f"Unexpected error while accessing database\
                      '{db_dict['name']}': {str(e)}"
            )


def write_parquet_to_s3(
    df: pd.DataFrame, file_path: str, metadata: Metadata
) -> None:
    """Writes a DataFrame to S3 as a Parquet file with metadata."""
    writer.write(
        df=df,
        output_path=file_path,
        metadata=metadata,
        file_format="parquet",
    )
    logging.info(f"Parquet file written to '{file_path}'")


def delete_existing_glue_table(
    glue_client, db_name: str, table_name: str
) -> None:
    """Deletes a Glue table if it exists."""
    try:
        glue_client.delete_table(DatabaseName=db_name, Name=table_name)
        logging.info(f"Deleted existing table '{table_name}' in database '{db_name}'") # noqa
        time.sleep(5)  # Allow time for deletion to propagate
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityNotFoundException":
            logging.info(
                f"Failed to delete table '{table_name}'\
                      in database '{db_name}': {e}"
            )


def create_glue_table(
    glue_client,
    gc: GlueConverter,
    metadata: Metadata,
    db_dict: Dict[str, Union[str, None]]
) -> None:
    """Creates (or overwrites) a Glue table."""
    spec = gc.generate_from_meta(
        metadata,
        database_name=db_dict["name"],
        table_location=db_dict["table_location"],
    )
    glue_client.create_table(**spec)
    logging.info(
        f"Table '{db_dict['table_name']}' created \
            (or overwritten) in database '{db_dict['name']}'"
    )


def write_curated_table_to_s3(
    df: pd.DataFrame,
    metadata: Metadata,
    db_dict: Dict[str, Union[str, None]]
) -> None:
    """Writes a curated DataFrame to S3 and
    updates/creates the corresponding Glue table."""
    gc = GlueConverter()
    glue_client = boto3.client("glue")

    create_glue_database(glue_client, db_dict)

    file_path = os.path.join(
        db_dict["table_location"],
        f"{db_dict['table_name']}.parquet",
    )
    write_parquet_to_s3(df, file_path, metadata)

    delete_existing_glue_table(
        glue_client, db_dict["name"], db_dict["table_name"]
    )

    create_glue_table(glue_client, gc, metadata, db_dict)

    logging.info("Data successfully written to s3 bucket and Athena Table created") # noqa


def move_completed_files_to_raw_hist(
    land_folder: str,
    raw_hist_folder: str,
    mojap_extraction_ts: int
):
    """Moves completed files from the
    landing folder to the raw history folder."""
    land_files = get_filepaths_from_s3_folder(s3_folder_path=land_folder)
    if not land_files:
        logging.info(f"No files to move into the landing folder - {land_folder}") # noqa
        return

    target_path = os.path.join(raw_hist_folder,
                               f"dag_run_ts_{mojap_extraction_ts}")
    logging.info(f"Target path for moved files: {target_path}")

    try:
        wr.s3.copy_objects(
            paths=land_files,
            source_path=land_folder,
            target_path=target_path,
        )
        logging.info(
            "Files successfully moved from the landing folder to the raw history folder."  # noqa
        )

        wr.s3.delete_objects(path=land_files)
        logging.info(f"Successfully deleted files in {land_folder}")
    except (BotoCoreError, ClientError) as error:
        logging.info(f"Failed to move or delete files: {error}")
    except Exception as e:
        logging.info(f"An unexpected error occurred: {e}")
