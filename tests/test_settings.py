import os
import pytest

example_env_vars = {
    "AWS_REGION": "eu-west-1",
    "MOJAP_EXTRACTION_TS": "1689866369",
    "MOJAP_IMAGE_VERSION": "v0.0.0",
    "TABLES": "TABLE1",
    "LANDING_FOLDER": "s3://a-bucket/a-land-folder/",
    "CURATED_FOLDER": "s3://a-bucket/a-curated-folder/",
    "RAW_HIST_FOLDER": "s3://a-bucket/a-raw-hist-folder/",
    "METADATA_FOLDER": "s3://a-bucket/a-metadata-folder/",
}

example_correct_settings = {
    "AWS_REGION": "eu-west-1",
    "MOJAP_EXTRACTION_TS": 1689866369,
    "MOJAP_IMAGE_VERSION": "v0.0.0",
    "TABLES": "TABLE1",
    "LANDING_FOLDER": "s3://a-bucket/a-land-folder/",
    "CURATED_FOLDER": "s3://a-bucket/a-curated-folder/",
    "RAW_HIST_FOLDER": "s3://a-bucket/a-raw-hist-folder/",
    "METADATA_FOLDER": "s3://a-bucket/a-metadata-folder/",
}


def set_example_settings():
    for setting in example_correct_settings:
        try:
            del os.environ[setting]
        except KeyError as error_str:
            print(f"Tidying up example settings. No env var '{error_str}' to delete")
    for setting in example_env_vars:
        os.environ[setting] = example_env_vars[setting]

def test_example_settings_with_tables():
    set_example_settings()
    this_example = dict(example_correct_settings)
    # Remove one of TABLE_PREFIX and TABLES, can't have both set
    this_example["TABLES"] = None
    del os.environ["TABLES"]
    from scripts.config import Settings
    assert Settings().model_dump() == this_example
