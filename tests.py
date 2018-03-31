import sys

import click
import pandas as pd
import pytest
from click import Context
from click.testing import CliRunner
from pandas.util.testing import assert_frame_equal
from sqlalchemy import Integer, Column, VARCHAR, DECIMAL
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from etl import cashflows, cli

Base = declarative_base()


def normalize_str(s: str) -> str:
    """Remove tabs, spaces, and new lines from a string"""
    return (
        s.replace("\t", "")
        .replace("\n", "")
        .replace("\r", "")
        .replace(" ", "")
    )


class InsuranceRow(Base):
    __tablename__ = 'staging'

    id = Column(Integer, primary_key=True)
    AGENCY_ID = Column(Integer)
    STAT_PROFILE_DATE_YEAR = Column(Integer)
    PROD_ABBR = Column(VARCHAR(10))
    AGENCY_APPOINTMENT_YEAR = Column(Integer)
    VENDOR = Column(VARCHAR(80))
    PROD_LINE = Column(VARCHAR(2))
    WRTN_PREM_AMT = Column(DECIMAL(19, 2))
    PRD_ERND_PREM_AMT = Column(DECIMAL(19, 2))
    PRD_INCRD_LOSSES_AMT = Column(DECIMAL(19, 2))


@pytest.fixture()
def test_db() -> Engine:
    engine = create_engine("sqlite:///")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    session.add_all([
        InsuranceRow(AGENCY_ID=3, STAT_PROFILE_DATE_YEAR=2015, PROD_ABBR="123", AGENCY_APPOINTMENT_YEAR=2015, VENDOR="ABC", PROD_LINE="CL", WRTN_PREM_AMT=100, PRD_ERND_PREM_AMT=90, PRD_INCRD_LOSSES_AMT=0),
        InsuranceRow(AGENCY_ID=3, STAT_PROFILE_DATE_YEAR=2015, PROD_ABBR="123", AGENCY_APPOINTMENT_YEAR=2015, VENDOR="DEF", PROD_LINE="CL", WRTN_PREM_AMT=110, PRD_ERND_PREM_AMT=110, PRD_INCRD_LOSSES_AMT=20),
        InsuranceRow(AGENCY_ID=3, STAT_PROFILE_DATE_YEAR=2017, PROD_ABBR="456", AGENCY_APPOINTMENT_YEAR=2015, VENDOR="ABC", PROD_LINE="PL", WRTN_PREM_AMT=120, PRD_ERND_PREM_AMT=120, PRD_INCRD_LOSSES_AMT=0),
        InsuranceRow(AGENCY_ID=3, STAT_PROFILE_DATE_YEAR=2015, PROD_ABBR="123", AGENCY_APPOINTMENT_YEAR=2014, VENDOR="ABC", PROD_LINE="CL", WRTN_PREM_AMT=130, PRD_ERND_PREM_AMT=0, PRD_INCRD_LOSSES_AMT=100),
        InsuranceRow(AGENCY_ID=4, STAT_PROFILE_DATE_YEAR=2014, PROD_ABBR="456", AGENCY_APPOINTMENT_YEAR=2015, VENDOR="ABC", PROD_LINE="CL", WRTN_PREM_AMT=140, PRD_ERND_PREM_AMT=130, PRD_INCRD_LOSSES_AMT=0)
    ])
    session.commit()
    return engine


def test_cash_flows(test_db: Engine):
    runner = CliRunner()
    actual = runner.invoke(cli, ("cashflows", "3"), obj={"engine": test_db})
    expected = """
        STAT_PROFILE_DATE_YEAR  2015   2017
        AGENCY_ID PROD_ABBR                
        3         123           80.0    0.0
                  456            0.0  120.0
    """
    assert actual.exit_code == 0
    assert normalize_str(expected) in normalize_str(actual.output)


def test_profitability(test_db: Engine):
    runner = CliRunner()
    actual = runner.invoke(cli, ("profitability", "3", "2015"), obj={"engine": test_db})
    expected = """
                   WRTN_PREM_AMT
        PROD_ABBR               
        123                  340
    """
    print(actual.output)
    assert actual.exit_code == 0
    assert normalize_str(expected) in normalize_str(actual.output)


def test_load(test_db: Engine):
    runner = CliRunner()
    actual = runner.invoke(cli, ("load",), obj={"engine": test_db})
    assert actual.exit_code == 0


if __name__ == '__main__':
    pytest.main(sys.argv)




