import logging
import os
import sys
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Dict, Optional, Union
from urllib.parse import urlparse

import awswrangler as wr
import boto3
import pandas as pd
import pyarrow.parquet as pq
from arrow_pd_parser import reader, writer
from botocore.exceptions import BotoCoreError, ClientError
from config import Settings
from dataengineeringutils3.s3 import (
    get_filepaths_from_s3_folder,
)
from mojap_metadata import Metadata
from mojap_metadata.converters.glue_converter import (
    GlueConverter,
)


def setup_logging(
    log_file: str,
    log_level: int = logging.INFO,
    max_file_size: int = 10 * 1024 * 1024,
    backup_count: int = 5,
):
    """
    Set up logging configuration with both file and console handlers.

    Args:
    log_file (str): Path to the log file.
    log_level (int): Logging level (default: logging.INFO).
    max_file_size (int): Maximum size of each log file in bytes (default: 10MB).
    backup_count (int): Number of backup log files to keep (default: 5).
    """
    # Create logs directory if it doesn't exist
    log_dir = os.path.dirname(log_file)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Create a logger
    logger = logging.getLogger()
    logger.setLevel(log_level)

    # Create formatters
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # File Handler (Rotating File Handler)
    file_handler = RotatingFileHandler(
        log_file, maxBytes=max_file_size, backupCount=backup_count
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(file_formatter)

    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(console_formatter)

    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def extract_data_to_s3(s3_path: str, local_base_path: str, logger: logging.Logger):
    """Extract data from local directory to S3."""
    for root, _, files in os.walk(local_base_path):
        for file in files:
            if file.endswith(".parquet"):
                file_path = os.path.join(root, file)
                s3_file_path = os.path.join(s3_path, file)
                try:
                    wr.s3.upload(file_path, s3_file_path)
                    logger.info(f"Uploaded {file} to {s3_path}")
                except Exception as e:
                    logger.error(
                        f"Failed to upload {file} to {s3_path}. Error: {str(e)}"
                    )

    logger.info("Extraction complete")
    # print("Source data extraction from Local to S3 landing folder complete")


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
            if partitions and not any(partition in obj.key for partition in partitions):
                continue
            parquet_files.append(f"s3://{bucket_name}/{obj.key}")
    return parquet_files


def read_parquet_file_to_dataframe(
    parquet_file: str,
) -> pd.DataFrame:
    """Reads a single Parquet file into a Pandas DataFrame."""
    return pq.ParquetDataset(parquet_file).read().to_pandas()


def load_data_from_s3(
    s3_path: str, logger: logging.Logger, partitions: Optional[Dict[str, str]] = None
) -> pd.DataFrame:
    """Loads and concatenates Parquet data from S3."""
    bucket_name, prefix = s3_path.replace("s3://", "").split("/", 1)
    parquet_files = list_parquet_files_in_s3(bucket_name, prefix, partitions)
    dfs = [read_parquet_file_to_dataframe(file) for file in parquet_files]
    full_df = pd.concat(dfs, ignore_index=True)
    logger.info("Loading data from landing bucket")
    # print("Loading data from landing bucket ...")
    return full_df


def load_metadata(metadata_folder: str, table_name: str) -> Metadata:
    """Load metadata from S3."""
    metadata_path = os.path.join(metadata_folder, f"{table_name}.json")
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


def update_metadata(metadata: Metadata, logger: logging.Logger) -> Metadata:
    """Updates metadata with new columns."""
    new_columns = get_new_columns_definition()
    for new_column in new_columns:
        metadata.update_column(new_column)
    logger.info("Metadata updated")
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


def cast_columns_to_correct_types(df: pd.DataFrame, metadata: Metadata) -> pd.DataFrame:
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
    logger: logging.Logger,
) -> pd.DataFrame:
    """Adds all MOJAP-specific columns to the DataFrame."""
    df["mojap_start_datetime"] = pd.to_datetime(df["Source extraction date"])
    df["mojap_image_tag"] = mojap_image_version
    df["mojap_raw_filename"] = "people-100000.csv"  # Consider making this dynamic
    df["mojap_task_timestamp"] = pd.to_datetime(mojap_extraction_ts, unit="s")
    logger.info("Data transformation")
    # print("Transforming data ...")
    return df


def create_glue_database(
    glue_client, db_dict: Dict[str, Union[str, None]], logger: logging.Logger
) -> None:
    """Creates a Glue database if it doesn't exist."""
    try:
        glue_client.get_database(Name=db_dict["name"])
        logger.info(f"Database '{db_dict['name']}' already exists")
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
                logger.info(f"Created Glue database '{db_dict['name']}'")
            except Exception as create_error:
                logger.error(
                    f"Failed to create database '{db_dict['name']}'. Error: {str(create_error)}"
                )
        else:
            logger.error(
                f"Unexpected error while accessing database '{db_dict['name']}': {str(e)}"
            )


def write_parquet_to_s3(
    df: pd.DataFrame, file_path: str, metadata: Metadata, logger: logging.Logger
) -> None:
    """Writes a DataFrame to S3 as a Parquet file with metadata."""
    writer.write(
        df=df,
        output_path=file_path,
        metadata=metadata,
        file_format="parquet",
    )
    logger.info(f"Parquet file written to '{file_path}'")


def delete_existing_glue_table(
    glue_client, db_name: str, table_name: str, logger: logging.Logger
) -> None:
    """Deletes a Glue table if it exists."""
    try:
        glue_client.delete_table(DatabaseName=db_name, Name=table_name)
        logger.info(f"Deleted existing table '{table_name}' in database '{db_name}'")
        time.sleep(5)  # Allow time for deletion to propagate
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityNotFoundException":
            logger.error(
                f"Failed to delete table '{table_name}' in database '{db_name}': {e}"
            )


def create_glue_table(
    glue_client,
    gc: GlueConverter,
    metadata: Metadata,
    db_dict: Dict[str, Union[str, None]],
    logger: logging.Logger,
) -> None:
    """Creates (or overwrites) a Glue table."""
    spec = gc.generate_from_meta(
        metadata,
        database_name=db_dict["name"],
        table_location=db_dict["table_location"],
    )
    glue_client.create_table(**spec)
    logger.info(
        f"Table '{db_dict['table_name']}' created (or overwritten) in database '{db_dict['name']}'"
    )


def write_curated_table_to_s3(
    df: pd.DataFrame,
    metadata: Metadata,
    db_dict: Dict[str, Union[str, None]],
    logger: logging.Logger,
) -> None:
    """Writes a curated DataFrame to S3 and updates/creates the corresponding Glue table."""
    gc = GlueConverter()
    glue_client = boto3.client("glue")

    create_glue_database(glue_client, db_dict, logger)

    file_path = os.path.join(
        db_dict["table_location"],
        f"{db_dict['table_name']}.parquet",
    )
    write_parquet_to_s3(df, file_path, metadata, logger)

    delete_existing_glue_table(
        glue_client, db_dict["name"], db_dict["table_name"], logger
    )

    create_glue_table(glue_client, gc, metadata, db_dict, logger)

    logger.info("Data successfully written to s3 bucket and Athena Table created")


def move_completed_files_to_raw_hist(
    land_folder: str,
    raw_hist_folder: str,
    mojap_extraction_ts: int,
    logger: logging.Logger,
):
    """Moves completed files from the landing folder to the raw history folder."""
    land_files = get_filepaths_from_s3_folder(s3_folder_path=land_folder)
    if not land_files:
        logger.info(f"No files to move into the landing folder - {land_folder}")
        return

    target_path = os.path.join(raw_hist_folder, f"dag_run_ts_{mojap_extraction_ts}")
    logger.info(f"Target path for moved files: {target_path}")

    try:
        wr.s3.copy_objects(
            paths=land_files,
            source_path=land_folder,
            target_path=target_path,
        )
        logger.info(
            "Files successfully moved from the landing folder to the raw history folder."
        )

        wr.s3.delete_objects(path=land_files)
        logger.info(f"Successfully deleted files in {land_folder}")

        # print(
        #     "Raw data moved from Landing folder - Raw History"
        # )
    except (BotoCoreError, ClientError) as error:
        logger.error(f"Failed to move or delete files: {error}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
