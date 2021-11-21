from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from pyspark.sql.functions import monotonically_increasing_id


def check_args(left_columns, right_columns, on, by, direction):
    assert on in right_columns and on in left_columns
    if by:
        assert by in right_columns and by in left_columns
        assert len(right_columns) > 2
    else:
        assert len(right_columns) > 1

    assert direction in ['backwards', 'nearest', 'forwards']


def apply_window(df, direction: str, on: str, by: str = None):
    a_left_column = [c for c in df.columns if c not in [on, by, 'id']][0]
    print(a_left_column)

    if direction == 'backwards':

        df = df.orderBy([by, on, a_left_column], ascending=[True, True, False])
        df.show()
        grouper = Window.partitionBy(by).orderBy(on) if by else Window.orderBy(on)
        w = grouper.rowsBetween(Window.unboundedPreceding, -1)

        df = df.withColumn('id', f.last('id', ignorenulls=True).over(w))

    elif direction == 'forwards':
        df = df.orderBy([by, on, a_left_column], ascending=[True, True, True])
        df.show()
        grouper = Window.partitionBy(by).orderBy(on) if by else Window.orderBy(on)
        w = grouper.rowsBetween(1, Window.unboundedFollowing)
        df = df.withColumn('id', f.first('id', ignorenulls=True).over(w))

    df = df.filter(~f.isnull(a_left_column))
    return df


def merge_asof(left: DataFrame, right: DataFrame, on: str, by: str=None, direction='backwards'):
    check_args(left.columns, right.columns, on, by, direction)
    right = right.orderBy([by, on]) if by else right.orderBy([on])
    # add unique id for each row to join on later
    right = right.select("*").withColumn("id", monotonically_increasing_id())
    right_temp = right.select([by, 'id', on]) if by else right.select(['id', on])
    # union by name, replace missing values with Null
    for col in left.columns:
        if col not in right_temp.columns:
            right_temp = right_temp.withColumn(col, f.lit(None))
    for col in right_temp.columns:
        if col not in left.columns:
            left = left.withColumn(col, f.lit(None))
    union = left.unionByName(right_temp)
    # pick any column in right that is not the by, on or id
    result = apply_window(union, direction, on, by)
    right_still_to_join = right.drop(by, on) if by else right.drop(on)
    result = result.join(right_still_to_join, on='id', how='left')
    result = result.drop('id')
    result.show()
    return result
