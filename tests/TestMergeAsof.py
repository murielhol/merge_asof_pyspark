import findspark
findspark.init()
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StringType, StructField, DateType, IntegerType, DoubleType
import pytest
from src.merge_asof_pyspark.merge_asof import merge_asof

sc = SparkContext(appName="MyFirstApp")
sql_session = SQLContext(sc)
spark = sql_session.sparkSession


class TestMergeAsof(object):

    @pytest.fixture()
    def get_left_and_right(self):

        left = [
            ("a", "2019-01-01", 1, "cat"),
            ("a", "2019-01-02", 2, "cat"),
            ("a", "2019-01-03", 3, "horse"),
            ("b", "2019-01-01", 1, "dog"),
            ("b", "2019-01-02", 2, "cat"),
            ("b", "2019-01-04", 4, "mouse"),
        ]
        schema = StructType([
            StructField('group', StringType(), True),
            StructField('datetime', StringType(), True),
            StructField('X', IntegerType(), True),
            StructField('animal', StringType(), True)

        ])
        left = spark.createDataFrame(data=left, schema=schema)
        left = left.withColumn("datetime", left['datetime'].cast(DateType()))

        right = [
            ("a", "2019-01-04", 4, 44.4),
            ("b", "2019-01-03", 3, 33.3),
            ("a", "2019-01-01", 1, 11.1),
            ("b", "2019-01-01", 1, 11.1),
            ("a", "2019-01-02", 2, 22.2),
            ("b", "2019-01-02", 2, 22.2),
            ("b", "2019-01-05", 5, 55.5),

        ]
        schema = StructType([
            StructField('group', StringType(), True),
            StructField('datetime', StringType(), True),
            StructField('Z', IntegerType(), True),
            StructField('Y', DoubleType(), True)
        ])

        right = spark.createDataFrame(data=right, schema=schema)
        right = right.withColumn("datetime", right['datetime'].cast(DateType()))

        return left, right

    def test_backwards(self, get_left_and_right):
        left, right = get_left_and_right
        result = merge_asof(left, right, on='datetime', by='group', direction='backwards')
        expected = [
            ("a", "2019-01-01", 1, "cat", None, None),
            ("a", "2019-01-02", 2, "cat", 1, 11.1),
            ("a", "2019-01-03", 3, "horse", 2, 22.2),
            ("b", "2019-01-01", 1, "dog", None, None),
            ("b", "2019-01-02", 2, "cat", 1, 11.1),
            ("b", "2019-01-04", 4, "mouse", 3, 33.3),
        ]
        schema = StructType([
            StructField('group', StringType(), True),
            StructField('datetime', StringType(), True),
            StructField('X', IntegerType(), True),
            StructField('animal', StringType(), True),
            StructField('Z', IntegerType(), True),
            StructField('Y', DoubleType(), True)

        ])
        expected = spark.createDataFrame(data=expected, schema=schema)
        expected = expected.withColumn("datetime", expected['datetime'].cast(DateType()))
        assert result.orderBy(['group', 'datetime']).collect() == expected.orderBy(['group', 'datetime']).collect()

    def test_forwards(self, get_left_and_right):
        left, right = get_left_and_right
        result = merge_asof(left, right, on='datetime', by='group', direction='forwards')
        expected = [
            ("a", "2019-01-01", 1, "cat", 2, 22.2),
            ("a", "2019-01-02", 2, "cat", 4, 44.4),
            ("a", "2019-01-03", 3, "horse", 4, 44.4),
            ("b", "2019-01-01", 1, "dog", 2, 22.2),
            ("b", "2019-01-02", 2, "cat", 3, 33.3),
            ("b", "2019-01-04", 4, "mouse", 5, 55.5),
        ]
        schema = StructType([
            StructField('group', StringType(), True),
            StructField('datetime', StringType(), True),
            StructField('X', IntegerType(), True),
            StructField('animal', StringType(), True),
            StructField('Z', IntegerType(), True),
            StructField('Y', DoubleType(), True)

        ])
        expected = spark.createDataFrame(data=expected, schema=schema)
        expected = expected.withColumn("datetime", expected['datetime'].cast(DateType()))
        assert result.orderBy(['group', 'datetime']).collect() == expected.orderBy(['group', 'datetime']).collect()

    def test_nearest(self, get_left_and_right):
        left, right = get_left_and_right
        result = merge_asof(left, right, on='datetime', by='group', direction='nearest')
        expected = [
            ("a", "2019-01-01", 1, "cat", 2, 22.2),
            ("a", "2019-01-02", 2, "cat", 4, 44.4),
            ("a", "2019-01-03", 3, "horse", 4, 44.4),
            ("b", "2019-01-01", 1, "dog", 2, 22.2),
            ("b", "2019-01-02", 2, "cat", 3, 33.3),
            ("b", "2019-01-04", 4, "mouse", 4, 44.4),
        ]
        schema = StructType([
            StructField('group', StringType(), True),
            StructField('datetime', StringType(), True),
            StructField('X', IntegerType(), True),
            StructField('animal', StringType(), True),
            StructField('Z', IntegerType(), True),
            StructField('Y', DoubleType(), True)

        ])
        expected = spark.createDataFrame(data=expected, schema=schema)
        expected = expected.withColumn("datetime", expected['datetime'].cast(DateType()))
        assert result.orderBy(['group', 'datetime']).collect() == expected.orderBy(['group', 'datetime']).collect()

