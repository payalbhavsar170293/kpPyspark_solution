from chispa.schema_comparer import *
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from dataframe_utils.transform import rename_column, filter_data

# Create a SparkSession for testing
spark = SparkSession.builder \
    .appName("PySparkTest") \
    .master("local[*]") \
    .getOrCreate()


# Test for rename_column function
def test_rename_column():
    data = [("1", "john"), ("2", "alice"), ("3", "kora")]
    df = spark.createDataFrame(data, ["id", "name"])

    result_df = rename_column(df, 'id', 'identity_number')
    result_df_schema = result_df.schema.names

    expected_schema = ["identity_number", "name"]

    assert_schema_equality(result_df_schema, expected_schema)


# Test for filter_data function
def test_filter_data():
    data = [("1", "john"), ("2", "alice"), ("3", "kora")]
    df = spark.createDataFrame(data, ["id", "name"])

    result_df = filter_data(df, 'name', 'alice')

    assert_df_equality(result_df, df.filter(df['name'] == 'alice'))
