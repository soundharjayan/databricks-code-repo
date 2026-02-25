from databricks.sdk.runtime import spark
from pyspark.sql import DataFrame
from wd36project import taxis


def test_find_all_taxis():
    results = taxis.find_all_taxis()
    assert results.count() > 5
