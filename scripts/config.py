import os
import re
# import dotenv
from typing import List, Optional, Union

from pydantic import model_validator
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    AWS_REGION: str = "eu-west-1"
    MOJAP_EXTRACTION_TS: int 
    MOJAP_IMAGE_VERSION: str

    TABLES: Optional[Union[str, List[str]]] = None

    LANDING_FOLDER: Optional[str] = None
    RAW_HIST_FOLDER: Optional[str] = None
    CURATED_FOLDER: Optional[str] = None
    METADATA_FOLDER: Optional[str] = None
    LOG_FOLDER: Optional[str] = 'logs' # None

    # New fields
    LOG_FILE: Optional[str] = None
    LOCAL_BASE_PATH: Optional[str] = "data/example-data"
    DB_NAME: Optional[str]  = "dami_intro_project"
    DB_DESCRIPTION: Optional[str] = "database with data from people parquet"


    @model_validator(mode="before")
    def check_land_and_or_meta(cls, values):
        """At least one of LANDING_FOLDER and METADATA_FOLDER must be
        set"""
        if (values.get("LANDING_FOLDER") is None) and (
            values.get("METADATA_FOLDER") is None
        ):
            raise ValueError(
                "At least one of LANDING_FOLDER or METADATA_FOLDER is required"
            )
        return values

    @classmethod
    def string_match(cls, strg: str,
                     match=re.compile(r'[A-Z0-9]+(_$)').match) -> bool:
        """
        Returns false if strg is NOT stringType
        AND
        Returns True only if pattern consists of Uppercase Alphanumerics
        followed by an Underscore.
        """
        if not isinstance(strg, str):
            return False
        return bool(match(strg))


print("Instantiating settings ...")