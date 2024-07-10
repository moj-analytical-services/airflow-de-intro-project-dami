import os
import logging

import time
from datetime import datetime

import pandas as pd
import awswrangler as wr
import pyarrow.parquet as pq

from typing import Dict, Optional, Union

import boto3
from botocore.exceptions import ClientError, BotoCoreError

from urllib.parse import urlparse
from mojap_metadata import Metadata
from mojap_metadata.converters.glue_converter import (
    GlueConverter,
)
from arrow_pd_parser import reader, writer

from config import settings
from utils import s3_path_join
from dataengineeringutils3.s3 import (
    get_filepaths_from_s3_folder,
)


# Set up logging
logging.basicConfig(
    filename="data_pipeline.log", # f"{settings.LOGS_FOLDER}/data_pipeline.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def extract_data_to_s3():

    s3_path = settings.LANDING_FOLDER
    base = os.path.join(os.getcwd(), 'data/example-data')

    for root, _, files in os.walk(base):
        for file in files:
            if file.endswith('.parquet'):
                file_path = os.path.join(root, file)
                s3_file_path = os.path.join(s3_path, file)
                wr.s3.upload(file_path, s3_file_path)
                logging.info(f"Uploading {file} to {s3_path}")

    logging.info("Extraction complete")
    print("Source data extraction from Repo to landing folder complete")

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
            if partitions and not any(
                partition in obj.key
                for partition in partitions
            ):
                continue
            parquet_files.append(
                f"s3://{bucket_name}/{obj.key}"
            )
    return parquet_files


def read_parquet_file_to_dataframe(
                                    parquet_file: str,
                                ) -> pd.DataFrame:
    """Reads a single Parquet file into a Pandas DataFrame."""
    return (
        pq.ParquetDataset(parquet_file).read().to_pandas()
    )


def load_data_from_s3(
                    partitions: Optional[Dict[str, str]] = None
                ) -> pd.DataFrame:
    """Loads and concatenates Parquet data from S3."""
    s3_path = settings.LANDING_FOLDER
    bucket_name, prefix = (
        (s3_path).replace("s3://", "").split("/", 1)
    )

    parquet_files = list_parquet_files_in_s3(
        bucket_name, prefix, partitions
    )

    dfs = [
        read_parquet_file_to_dataframe(file)
        for file in parquet_files
    ]

    full_df = pd.concat(dfs, ignore_index=True)
    print("Loading data from landing bucket ...")
    return full_df


def load_metadata() -> Metadata:
    metadata = s3_path_join(
        settings.METADATA_FOLDER, f"{settings.TABLES}.json"
        )
    metadata = Metadata.from_json(metadata)
    # Affirm Table name
    metadata.name = settings.TABLES
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
        if column_type == "timestamp(ms)":
            df[column_name] = pd.NaT
        elif column_type == "string":
            df[column_name] = ""
        else:
            df[column_name] = pd.NA
    if column_type == "timestamp(ms)":
        df[column_name] = pd.to_datetime(
            df[column_name],
            format=(
                datetime_format
                if datetime_format
                else "%Y-%m-%dT%H:%M:%S"
            ),
        )
    else:
        df[column_name] = df[column_name].astype(
            column_type
        )
    return df


def cast_columns_to_correct_types(
                                    df: pd.DataFrame,
                                ) -> pd.DataFrame:
    """Casts columns in a DataFrame to types defined in metadata."""
    metadata = load_metadata()
    metadata = update_metadata(metadata)

    for column in metadata.columns:
        df = cast_column_to_type(
            df,
            column["name"],
            column["type"],
            column.get("datetime_format"),
        )

    return df


def add_mojap_start_datetime_column(
                                    df: pd.DataFrame,
                                ) -> pd.DataFrame:
    """Adds 'mojap_start_datetime' column based on 'Source extraction date'."""
    df["mojap_start_datetime"] = pd.to_datetime(
        df["Source extraction date"]
    )
    return df


def add_static_mojap_columns(
                            df: pd.DataFrame,
                        ) -> pd.DataFrame:
    """Adds static MOJAP columns to the DataFrame."""
    df["mojap_image_tag"] = settings.MOJAP_IMAGE_VERSION
    df["mojap_raw_filename"] = (
        "people-100000.csv"  # Consider making this dynamic
    )
    df["mojap_task_timestamp"] = pd.to_datetime(
        settings.MOJAP_EXTRACTION_TS, unit="s"
    )
    print("Transforming data ...")
    return df


def add_mojap_columns_to_dataframe(
                                    df: pd.DataFrame,
                                ) -> pd.DataFrame:
    """Adds all MOJAP-specific columns to the DataFrame."""
    df = add_mojap_start_datetime_column(df)
    df = add_static_mojap_columns(df)
    return df


def create_glue_database(
                            glue_client, db_dict: Dict[str, Union[str, None]]
                        ) -> None:
    """Creates a Glue database if it doesn't exist."""
    try:
        glue_client.get_database(Name=db_dict["name"])
    except ClientError as e:
        if (
            e.response["Error"]["Code"]
            == "EntityNotFoundException"
        ):
            db_meta = {
                "DatabaseInput": {
                    "Name": db_dict["name"],
                    "Description": db_dict["description"],
                }
            }
            glue_client.create_database(**db_meta)
            logging.info(
                "Created Glue database '%s'",
                db_dict["name"],
            )
        else:
            logging.error(
                "Unexpected error while accessing database '%s': %s",
                db_dict["name"],
                e,
            )


def write_parquet_to_s3(
                        df: pd.DataFrame, file_path: str, metadata: "Metadata"
                    ) -> None:
    """Writes a DataFrame to S3 as a Parquet file with metadata."""
    writer.write(
        df=df,
        output_path=file_path,
        metadata=metadata,
        file_format="parquet",
    )
    logging.info("Parquet file written to '%s'", file_path)


def delete_existing_glue_table(
                                glue_client, db_name: str, table_name: str
                            ) -> None:
    """Deletes a Glue table if it exists."""
    try:
        glue_client.delete_table(
            DatabaseName=db_name, Name=table_name
        )
        logging.info(
            "Deleted existing table '%s' in database '%s'",
            table_name,
            db_name,
        )
        time.sleep(
            5
        )  # Allow time for deletion to propagate
    except ClientError as e:
        if (
            e.response["Error"]["Code"]
            != "EntityNotFoundException"
        ):
            logging.error(
                "Failed to delete table '%s' in database '%s': %s",
                table_name,
                db_name,
                e,
            )


def create_glue_table(
                        glue_client,
                        gc: "GlueConverter",
                        metadata: "Metadata",
                        db_dict: Dict[str, Union[str, None]],
                    ) -> None:
    """Creates (or overwrites) a Glue table."""
    spec = gc.generate_from_meta(
        metadata,
        database_name=db_dict["name"],
        table_location=db_dict["table_location"],
    )
    glue_client.create_table(**spec)
    logging.info(
        "Table '%s' created (or overwritten) in database '%s'",
        db_dict["table_name"],
        db_dict["name"],
    )


def write_curated_table_to_s3(df: pd.DataFrame, metadata=load_metadata()) -> None:
    """Writes a curated DataFrame to S3 and updates/creates the corresponding Glue table."""
    db_dict: Dict[str, Union[str, None]] = {
        "name": "dami_intro_project",
        "description": "database with data from people parquet",
        "table_name": settings.TABLES,
        "table_location": settings.CURATED_FOLDER,
    }

    metadata = update_metadata(metadata)
    gc = GlueConverter()
    glue_client = boto3.client("glue")

    create_glue_database(glue_client, db_dict)

    file_path = s3_path_join(
        db_dict["table_location"],
        f"{db_dict['table_name']}.parquet",
    )
    write_parquet_to_s3(df, file_path, metadata)

    delete_existing_glue_table(
        glue_client, db_dict["name"], db_dict["table_name"]
    )
    create_glue_table(glue_client, gc, metadata, db_dict)

    logging.info(
        "Data successfully written to s3 bucket and Athena Table created"
    )


def move_completed_files_to_raw_hist():
    """Moves completed files from the landing folder to the raw history folder."""
    land_folder = settings.LANDING_FOLDER
    raw_hist_folder = settings.RAW_HIST_FOLDER

    land_files = get_filepaths_from_s3_folder(
        s3_folder_path=land_folder
    )
    if not land_files:
        logging.info(
            "No files to move into the landing folder - %s",
            land_folder,
        )
        return

    target_path = s3_path_join(
        raw_hist_folder,
        f"dag_run_ts_{settings.MOJAP_EXTRACTION_TS}",
    )
    logging.info(
        "Target path for moved files: %s", target_path
    )

    try:
        wr.s3.copy_objects(
            paths=land_files,
            source_path=land_folder,
            target_path=target_path,
        )
        logging.info(
            "Files successfully moved from the landing folder to the raw history folder."
        )

        wr.s3.delete_objects(path=land_files)
        logging.info(
            "Successfully deleted files in %s", land_folder
        )

        print(
            (
                "Raw data moved from Landing folder - Raw History"
            )
        )
    except (BotoCoreError, ClientError) as error:
        logging.error(
            "Failed to move or delete files: %s", error
        )
    except Exception as e:
        logging.error("An unexpected error occurred: %s", e)
