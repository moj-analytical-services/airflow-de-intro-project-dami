import argparse
import os
import sys

from scripts.config import settings
from scripts.run import run_data_pipeline


# sys.path.append(os.path.abspath(os.path.dirname(__file__)))

if settings.TABLES:
    run_data_pipeline()
else:
    print("Check the process")

# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(
#         description="Acquire the sample 'Peoples Data' from Land, curate and move to curated folder"
#     )
#     parser.add_argument("table", help="Table you want to curate")
#     args = parser.parse_args()

#     settings.TABLES = args.table

#     run_data_pipeline()