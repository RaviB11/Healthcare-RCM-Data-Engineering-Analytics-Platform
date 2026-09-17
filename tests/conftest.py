"""Shared pytest fixtures. One Spark session for the whole session."""
from __future__ import annotations

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (SparkSession.builder
               .appName("rcm-tests")
               .master("local[1]")
               .config("spark.driver.memory", "1g")
               .config("spark.sql.shuffle.partitions", "1")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def rows_as_dicts(df, *cols):
    return [{c: r[c] for c in (cols or df.columns)} for r in df.collect()]
