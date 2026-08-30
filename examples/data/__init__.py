"""Load demo CSV data from ``examples/data/day1/`` and ``examples/data/day2/``.

Each function takes a ``SparkSession`` and returns a ``DataFrame`` with the
schema defined by the CSV header.

Usage::

    from examples.data import customers_day1

    df = customers_day1(spark)
    df.show()
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

_DATA = Path(__file__).resolve().parent


def _load(day: str, table: str, spark: SparkSession) -> DataFrame:
    return spark.read.option("header", True).csv(str(_DATA / day / f"{table}.csv"))


# -- Day 1 -------------------------------------------------------------------

def customers_day1(spark: SparkSession) -> DataFrame:
    return _load("day1", "customers", spark)


def products_day1(spark: SparkSession) -> DataFrame:
    return _load("day1", "products", spark)


def orders_day1(spark: SparkSession) -> DataFrame:
    return _load("day1", "orders", spark)


def order_items_day1(spark: SparkSession) -> DataFrame:
    return _load("day1", "order_items", spark)


def employees_day1(spark: SparkSession) -> DataFrame:
    return _load("day1", "employees", spark)


# -- Day 2 -------------------------------------------------------------------

def customers_day2(spark: SparkSession) -> DataFrame:
    return _load("day2", "customers", spark)


def products_day2(spark: SparkSession) -> DataFrame:
    return _load("day2", "products", spark)


def orders_day2(spark: SparkSession) -> DataFrame:
    return _load("day2", "orders", spark)


def order_items_day2(spark: SparkSession) -> DataFrame:
    return _load("day2", "order_items", spark)


def employees_day2(spark: SparkSession) -> DataFrame:
    return _load("day2", "employees", spark)
