import logging
import os
import sys

import datetime

import click
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


def delete_db() -> None:
    logger.info("Wiping the database...")
    try:
        db_path = os.path.join(ROOT_DIR, "insurance.db")
        if os.path.exists(db_path):
            os.remove(db_path)
    except Exception as e:
        logger.error(f"There was an error connecting to the database: {e}")
        raise


def connect() -> Engine:
    logger.info("Connecting to insurance.db...")
    try:
        db_path = os.path.join(ROOT_DIR, "insurance.db")
        return create_engine(f"sqlite:///{db_path}")
    except Exception as e:
        logger.error(f"Unable to connect to the database: {e}")
        raise


def export_df(name: str, df: pd.DataFrame) -> None:
    logger.info(f"Exporting {name} to csv...")
    try:
        folder = os.path.join(ROOT_DIR, "out")
        if not os.path.exists(folder):
            os.mkdir(folder)
    except Exception as e:
        logger.error(f"Unable to create out directory in the current folder: {e}")
        raise

    try:
        filepath = os.path.join(folder, f"{name}_{datetime.datetime.now().strftime('%Y-%m-%d')}.csv")
        df.to_csv(path_or_buf=filepath, chunksize=1000)
    except Exception as e:
        logger.error(f"There was an error exporting the '{name}' dataframe: {e}")
        raise
    else:
        logger.info(f"The {name} dataset was exported successfully.")


def load_staging() -> pd.DataFrame:
    logger.info("Loading the staging table...")
    csv_path = os.path.join(ROOT_DIR, "finalapi.csv")
    try:
        df = pd.read_csv(csv_path, sep=',')
        df.to_sql("staging", con=engine, if_exists="replace", index=False)
    except Exception as e:
        logger.error(f"There was an error loading '{csv_path}' to the staging table: {e}")
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


@click.group()
def main():
    pass


@main.command()
@click.argument("agency-id")
@click.option("--dest", "-d", default="stdout", help="Destination for the report, either 'csv' or 'stdout'")
def cashflows(agency_id: int, dest: str) -> pd.DataFrame:
    logger.info(f"Creating report for the last 5 years of net cash flows for an agency {agency_id}...")
    try:
        cash_flows = (
            pd.read_sql(sql="staging", con=engine)
            .query(f"AGENCY_ID == '{agency_id}'")
            .assign(net_cash_flows=lambda df: df.PRD_ERND_PREM_AMT - df.PRD_INCRD_LOSSES_AMT)
            .pivot_table(
                index=["AGENCY_ID", "PROD_ABBR"],
                columns=["STAT_PROFILE_DATE_YEAR"],
                values="net_cash_flows",
                aggfunc=np.sum
            )
            .iloc[:, -5:]
        )
        if dest == "stdout":
            print(cash_flows)
        elif dest == "csv":
            export_df(name=f"cash_flows_id-{agency_id}", df=cash_flows)
        else:
            logger.error("Unrecognized destination argument")
            raise KeyError(f"The destination {dest} is not a valid option.")
    except Exception as e:
        logger.error(f"There was an error exporting the net cash flows dataset: {e}")
        raise
    else:
        return cash_flows


@main.command()
@click.argument("agency-id")
@click.argument("year")
@click.option("--dest", "-d", default="stdout", help="Destination for the report, either 'csv' or 'stdout'")
def profitability(agency_id: int, year: int, dest: str) -> pd.DataFrame:
    logger.info(f"Creating profitability report for agency {agency_id} for {year}...")
    try:
        profitability = (
            pd.read_sql(sql="staging", con=engine)
            .query(f"AGENCY_ID == {agency_id} & STAT_PROFILE_DATE_YEAR == {year}")
            .pivot_table(values="WRTN_PREM_AMT", index="PROD_ABBR", aggfunc=np.sum)
            .sort_values(by="WRTN_PREM_AMT", ascending=False)
        )
        if dest == "stdout":
            print(profitability)
        elif dest == "csv":
            export_df(name=f"profitability_id-{agency_id}_yr-{year}", df=profitability)
        else:
            logger.error("Unrecognized destination argument")
            raise KeyError(f"The destination {dest} is not a valid option.")
    except Exception as e:
        logger.error(f"There was an error exporting the profitability dataset: {e}")
        raise
    else:
        return profitability


@main.command()
def load():
    logger.info("Loading the data warehouse...")
    delete_db()
    staging = load_staging()
    load_revenue_fact(staging=staging)
    load_agency_dim(staging=staging)
    load_product_dim(staging=staging)
    logger.info("The data warehouse was loaded successfully.")


if __name__ == '__main__':
    setup_logging(root_dir=ROOT_DIR)
    logger = logging.getLogger("root")
    engine = connect()
    main()

