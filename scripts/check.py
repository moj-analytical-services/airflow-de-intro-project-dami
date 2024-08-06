import os
import logging
import dotenv

from config import Settings, TEST_MODE
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

def main(settings, logger):
    """
    This function encapsulates the entire data processing pipeline, making it
    easier to understand and maintain. It clearly shows the flow of data
    from extraction to loading.
    """
    
    # setup_logging(settings.log_file)
    
    # Moves data from directory to S3 landing bucket
    if settings.LANDING_FOLDER:
        extract_data_to_s3(settings.LANDING_FOLDER,
                           settings.LOCAL_BASE_PATH,
                           logger)

    if settings.TABLES:
        df = load_data_from_s3(settings.LANDING_FOLDER, 
                               logger)

        metadata = load_metadata(settings.METADATA_FOLDER, 
                                 settings.TABLES)
        
        metadata = update_metadata(metadata, logger)

        df = cast_columns_to_correct_types(df, metadata)
        df = add_mojap_columns_to_dataframe(
            df,
            settings.MOJAP_IMAGE_VERSION,
            settings.MOJAP_EXTRACTION_TS, 
            logger
        )

        db_dict = {
            "name": settings.DB_NAME,
            "description": settings.DB_DESCRIPTION,
            "table_name": settings.TABLES,
            "table_location": settings.CURATED_FOLDER,
        }

        write_curated_table_to_s3(df, metadata, db_dict, logger)

        move_completed_files_to_raw_hist(
            settings.LANDING_FOLDER,
            settings.RAW_HIST_FOLDER,
            settings.MOJAP_EXTRACTION_TS, 
            logger
        )

# dotenv.load_dotenv(dotenv_path="dev.env")
# TEST_MODE = os.getenv("TEST_MODE", False)

if __name__ == "__main__":
    if TEST_MODE == True:
        settings = Settings(_env_file="dev.env")
        print(TEST_MODE, "TEST_MODE activated")
    else:
        settings = Settings()

    os.environ["AWS_REGION"] = settings.AWS_REGION
    os.environ["AWS_DEFAULT_REGION"] = settings.AWS_REGION

    # Add additional settings
    # settings.log_file = "data_pipeline.log"
    # settings.local_base_path = "data/example-data"
    # settings.db_name = "dami_intro_project"
    # settings.db_description = "database with data from people parquet"

    # Initialize logger
    logger = setup_logging(os.path.join(settings.LOG_FOLDER, 'data_pipeline.log'))

    main(settings, logger)