# -*- coding: utf-8 -*-
import logging
import os
import sys

import numpy as np
import pandas as pd
from sqlalchemy import INTEGER, DECIMAL, VARCHAR, CHAR
from sqlalchemy.engine import create_engine, Engine

from setuplogging import setup_logging

__version__ = '0.1'

try:
    ROOT_DIR = os.path.dirname(sys.argv[0])
except:
    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


def load_staging(csv_path: str) -> pd.DataFrame:
    logger.info("Loading the staging table...")
    try:
        df = pd.read_csv(csv_path, sep=',')
        df.to_sql("staging", con=engine, if_exists="replace")
    except Exception as e:
        logger.error(f"There was an error loading the staging table: {e}")
        raise
    else:
        logger.info("The staging table was successfully loaded.")
        return df


def load_product_dim(staging: pd.DataFrame) -> pd.DataFrame:
    logger.info("Loading the product_dim table...")
    try:
        df = staging[["PROD_ABBR", "PROD_LINE"]].drop_duplicates()
        df.to_sql("product_dim", con=engine, if_exists="replace")
    except Exception as e:
        logger.error(f"There was an error loading the product_dim table: {e}")
        raise
    else:
        logger.info("The product_dim table was successfully loaded.")
        return df


def load_revenue_fact(staging: pd.DataFrame) -> pd.DataFrame:
    logger.info("Loading the revenue_fact...")
    try:
        df = staging.pivot_table(
            index=[
                "AGENCY_ID",
                "PROD_ABBR",
                "STATE_ABBR",
                "STAT_PROFILE_DATE_YEAR"
            ],
            values=[
                "PRD_ERND_PREM_AMT",
                "POLY_INFORCE_QTY",
                "WRTN_PREM_AMT"
            ],
            aggfunc=np.sum
        )
        df.to_sql(
            name="revenue_fact",
            con=engine,
            if_exists="replace",
            dtype={
                "AGENCY_ID": INTEGER(),
                "PROD_ABBR": VARCHAR(40),
                "STATE_ABBR": CHAR(2),
                "STAT_PROFILE_DATE_YEAR": INTEGER(),
                "PRD_ERND_PREM_AMT": DECIMAL(19, 2),
                "POLY_INFORCE_QTY": DECIMAL(19, 2),
                "WRTN_PREM_AMT": DECIMAL(19, 2)
            }
        )
    except Exception as e:
        logger.error(f"There was an error loading the revenue_fact: {e}")
        raise
    else:
        logger.info("The revenue_fact was successfully loaded.")
        return df


def load_agency_dim(staging: pd.DataFrame) -> pd.DataFrame:
    logger.info("Loading agency_dim...")
    try:
        df = staging[[
            "AGENCY_ID",
            "PRIMARY_AGENCY_ID",
            "VENDOR",
            "ACTIVE_PRODUCERS",
            "AGENCY_APPOINTMENT_YEAR",
            "VENDOR_IND"
        ]].drop_duplicates()
        df.to_sql("agency_dim", con=engine, if_exists="replace")
    except Exception as e:
        logger.error(f"There was an error loading the agency_dim table: {e}")
    else:
        logger.info("The gency_dim was successfully loaded.")
        return df


def connect() -> Engine:
    """Wipe the database, create a new one and connect."""

    logger.info("Resetting the database...")
    db_path = os.path.join(ROOT_DIR, "insurance.db")
    try:
        if os.path.exists(db_path):
            os.remove(db_path)
        return create_engine(f"sqlite:///{db_path}")
    except Exception as e:
        logger.error(f"There was an error connecting to the database: {e}")
        raise


if __name__ == '__main__':
    setup_logging(root_dir=ROOT_DIR)
    logger = logging.getLogger("root")

    logger.info("Loading the data warehouse...")
    try:
        engine = connect()
        csv_path = "finalapi.csv"
        staging = load_staging(csv_path=csv_path)
        load_revenue_fact(staging=staging)
        load_agency_dim(staging=staging)
        load_product_dim(staging=staging)
    except Exception as e:
        logger.error(f"There was an error loading the data warehouse: {e}")
        raise
    else:
        logger.info("The data warehouse was loaded successfully.")
