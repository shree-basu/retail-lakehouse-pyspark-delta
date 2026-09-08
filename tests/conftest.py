from __future__ import annotations

import os
import sys

import pytest


def _configure_python_worker() -> None:
    """Isolate tests from a machine-wide Spark install and path with spaces."""

    os.environ.pop("SPARK_HOME", None)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ["PYSPARK_PYTHON"] = "python"
    executable_dir = os.path.dirname(sys.executable)
    if os.environ["PATH"].split(os.pathsep)[0] != executable_dir:
        os.environ["PATH"] = f"{executable_dir}{os.pathsep}{os.environ['PATH']}"


def pytest_sessionstart() -> None:
    _configure_python_worker()


@pytest.fixture(scope="session")
def spark():
    _configure_python_worker()
    from src.spark_session import create_spark_session

    # The first SparkContext in a Python process fixes the JVM classpath. Start
    # every Spark test from the Delta-configured factory so collection order
    # cannot create a plain Spark gateway that is missing the Delta JARs.
    session = create_spark_session("RetailLakehouse-ContractTests")
    session.conf.set("spark.sql.shuffle.partitions", "4")
    yield session
    session.stop()
