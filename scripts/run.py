from config import settings

from functions import (
    setup_logging,
    extract_data_to_s3,
    load_metadata,
    update_metadata,
    load_data_from_s3,
    cast_columns_to_correct_types,
    add_mojap_columns_to_dataframe,
    write_curated_table_to_s3,
    move_completed_files_to_raw_hist,
)

config = {
    "log_file": "data_pipeline.log",
    "local_base_path": "data/example-data",
    "db_name": "dami_intro_project",
    "db_description": "database with data from people parquet"
}

def main(settings, config=config):
    """
    This function encapsulates the entire data processing pipeline, making it
    easier to understand and maintain. It clearly shows the flow of data
    from extraction to loading.
    """

    # Moves data from directory to S3 landing bucket
    setup_logging(config["log_file"])

    if settings.LANDING_FOLDER:
        extract_data_to_s3(settings.LANDING_FOLDER, config["local_base_path"])

    if settings.TABLES:
        df = load_data_from_s3(settings.LANDING_FOLDER)

        metadata = load_metadata(settings.METADATA_FOLDER, settings.TABLES)
        metadata = update_metadata(metadata)

        df = cast_columns_to_correct_types(df, metadata)
        df = add_mojap_columns_to_dataframe(
            df,
            settings.MOJAP_IMAGE_VERSION,
            settings.MOJAP_EXTRACTION_TS,
        )

        db_dict = {
            "name": config["db_name"],
            "description": config["db_description"],
            "table_name": settings.TABLES,
            "table_location": settings.CURATED_FOLDER,
        }

        write_curated_table_to_s3(df, metadata, db_dict)

        move_completed_files_to_raw_hist(
            settings.LANDING_FOLDER,
            settings.RAW_HIST_FOLDER,
            settings.MOJAP_EXTRACTION_TS,
        )

if __name__ == "__main__":
    main(settings)#, config)