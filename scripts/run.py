import argparse
import logging
import os
import sys

from config import Settings
from functions import (
    add_mojap_columns_to_dataframe,
    cast_columns_to_correct_types,
    extract_data_to_s3,
    load_data_from_s3,
    load_metadata,
    move_completed_files_to_raw_hist,
    setup_logging,
    update_metadata,
    write_curated_table_to_s3,
)


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Data processing pipeline")
    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        default="prod",
        help="Specify the environment (dev or prod). Default is prod.",
    )
    return parser.parse_args()


def load_settings(env: str) -> Settings:
    """Load settings based on the specified environment."""
    if env == "dev":
        # dotenv.load_dotenv(dotenv_path="dev.env")
        return Settings(_env_file="dev.env")
    else:
        return Settings()


def setup_environment(settings: Settings) -> None:
    """Set up environment variables."""
    os.environ["AWS_REGION"] = settings.AWS_REGION
    os.environ["AWS_DEFAULT_REGION"] = settings.AWS_REGION


def main(settings: Settings, logger: logging.Logger) -> None:
    """
    Main function encapsulating the entire data processing pipeline.

    Args:
        settings (Settings): Configuration settings.
        logger (logging.Logger): Logger instance.
    """
    try:
        if settings.LANDING_FOLDER:
            extract_data_to_s3(
                settings.LANDING_FOLDER, settings.LOCAL_BASE_PATH, logger
            )

        if settings.TABLES:
            df = load_data_from_s3(settings.LANDING_FOLDER, logger)
            metadata = load_metadata(settings.METADATA_FOLDER, settings.TABLES)
            metadata = update_metadata(metadata, logger)
            df = cast_columns_to_correct_types(df, metadata)
            df = add_mojap_columns_to_dataframe(
                df, settings.MOJAP_IMAGE_VERSION, settings.MOJAP_EXTRACTION_TS, logger
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
                logger,
            )

        logger.info("Data processing pipeline completed successfully.")
    except Exception as e:
        logger.exception(
            f"An error occurred during the data processing pipeline: {str(e)}"
        )
        sys.exit(1)


if __name__ == "__main__":
    args = parse_arguments()

    try:
        settings = load_settings(args.env)
        setup_environment(settings)

        log_file = os.path.join(settings.LOG_FOLDER, "data_pipeline.log")
        logger = setup_logging(log_file)

        logger.info(f"Starting data processing pipeline in {args.env} environment.")

        main(settings, logger)

    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}", file=sys.stderr)
        sys.exit(1)