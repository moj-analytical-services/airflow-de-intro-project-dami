import argparse

from config import settings
from functions import (
    extract_data_to_s3,
    load_data_from_s3,
    cast_columns_to_correct_types,
    add_mojap_columns_to_dataframe,
    write_curated_table_to_s3,
    move_completed_files_to_raw_hist,
)

def run_data_pipeline():
    """
    This function encapsulates the entire data processing pipeline, making it
    easier to understand and maintain. It clearly shows the flow of data
    from extraction to loading.
    """
    
    df = load_data_from_s3()
    df = cast_columns_to_correct_types(df)
    df = add_mojap_columns_to_dataframe(df)
    write_curated_table_to_s3(df)
    move_completed_files_to_raw_hist()

if __name__ == "__main__":
    # Entry point for the script.
    # This ensures that the data pipeline is only executed when the script is
    # run directly (not imported as a module). The argparse module can be 
    # used here to handle command-line arguments, allowing for greater
    # flexibility in how the pipeline is executed.

    parser = argparse.ArgumentParser(description="Run data pipeline.")

    # Add arguments as needed, for example:
    # parser.add_argument("--env", default="dev", help="Environment to run in (dev, prod, etc.)")
    args = parser.parse_args()

    # Move the data to Landing folder
    extract_data_to_s3()

    if settings.TABLES:
        run_data_pipeline()
    else:
        print("Check the process")