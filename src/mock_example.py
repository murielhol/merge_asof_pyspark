import findspark  # this needs to be the first import
findspark.init()
from pyspark import SparkContext
from pyspark.sql import *
import pyspark.sql
from pyspark.sql.types import StructType, StringType, StructField, DateType
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from pyspark.sql.functions import monotonically_increasing_id

sc = SparkContext(appName="MyFirstApp")
sql_session = SQLContext(sc)
spark = sql_session.sparkSession


def get_left_and_right():

    left = [
        ("a", "2019-01-01", "1"),
        ("a", "2019-01-02", "2"),
        ("a", "2019-01-03", "3"),
        ("b", "2019-01-01", "1"),
        ("b", "2019-01-02", "2"),
        ("b", "2019-01-04", "4"),
    ]
    schema = StructType([
        StructField('Cow', StringType(), True),
        StructField('Datetime', StringType(), True),
        StructField('X', StringType(), True)
    ])
    left = spark.createDataFrame(data=left, schema=schema)
    left = left.withColumn("Datetime", left['Datetime'].cast(DateType()))

    right = [
        ("a", "2019-01-01", "1", "11"),
        ("a", "2019-01-02", "2", "22"),
        ("a", "2019-01-04", "4", "44"),
        ("b", "2019-01-01", "1", "11"),
        ("b", "2019-01-02", "2", "22"),
        ("b", "2019-01-03", "3", "33"),
        ("b", "2019-01-05", "5", "55"),

    ]
    schema = StructType([
        StructField('Cow', StringType(), True),
        StructField('Datetime', StringType(), True),
        StructField('Y', StringType(), True),
        StructField('Z', StringType(), True)
    ])

    right = spark.createDataFrame(data=right, schema=schema)
    right = right.withColumn("Datetime", right['Datetime'].cast(DateType()))

    return left, right



def merge_asof(left, right):
    # add unique id for each row to join on later
    right = right.select("*").withColumn("id", monotonically_increasing_id())
    right_temp = right.select(['Cow', 'id', 'Datetime'])

    # union
    for col in left.columns:
        if not col in right_temp.columns:
            right_temp = right_temp.withColumn(col, f.lit(None))
    for col in right_temp.columns:
        if not col in left.columns:
            left = left.withColumn(col, f.lit(None))
    union = left.unionByName(right_temp)

    # merge asof backwards
    w = Window.partitionBy('Cow').orderBy('Datetime').rowsBetween(Window.unboundedPreceding, -1)
    result = union.withColumn('id', f.last('id', True).over(w)).filter(~f.isnull('X'))

    # join rest of the data
    right_still_to_join = right.drop('Cow', 'Datetime')
    result: pyspark.sql.DataFrame = result.join(right_still_to_join, on='id', how='left')
    result.show()


left, right = get_left_and_right()
merge_asof(left, right)
