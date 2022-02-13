# Merge asof in pyspark

Pandas has a great function called [merged_asof](https://pandas.pydata.org/pandas-docs/version/0.25.0/reference/api/pandas.merge_asof.html).
This code is to get the same results with pure pyspark code. 



# explanation with example

Let us take 2 dataframes, ```left``` and ```right```:

```
left.show()
+-----+----------+---+------+
|group|  datetime|  X|animal|
+-----+----------+---+------+
|    a|2019-01-01|  1|   cat|
|    a|2019-01-02|  2|   cat|
|    a|2019-01-03|  3| horse|
|    b|2019-01-01|  1|   dog|
|    b|2019-01-02|  2|   cat|
|    b|2019-01-04|  4| mouse|
+-----+----------+---+------+

right.show()
+-----+----------+---+----+
|group|  datetime|  Z|   Y|
+-----+----------+---+----+
|    a|2019-01-04|  3|44.4|
|    b|2019-01-03|  3|33.3|
|    a|2019-01-01|  1|11.1|
|    b|2019-01-01|  1|11.1|
|    a|2019-01-02|  2|22.2|
|    b|2019-01-02|  2|22.2|
|    b|2019-01-05|  5|55.5|
+-----+----------+---+----+

```

We are going to join on ```datetime```, grouped by ```group``` in backwards direction.
So each row in ```left``` will be joined with the most recent but preceding row in ```right```.
Right will be ordered on group, datetime:

```
+-----+----------+---+----+
|group|  datetime|  Z|   Y|
+-----+----------+---+----+
|    a|2019-01-01|  1|11.1|
|    a|2019-01-02|  2|22.2|
|    a|2019-01-04|  3|44.4|
|    b|2019-01-01|  1|11.1|
|    b|2019-01-02|  2|22.2|
|    b|2019-01-03|  3|33.3|
|    b|2019-01-05|  5|55.5|
+-----+----------+---+----+
```

Then an id column is added to the right dataframe. Is does not matter what the id's are as long as it is monotonically increasing.

```

right.show()
+-----+----------+---+----+-----------+
|group|  datetime|  Z|   Y|         id|
+-----+----------+---+----+-----------+
|    a|2019-01-01|  1|11.1|          0|
|    a|2019-01-02|  2|22.2| 8589934592|
|    a|2019-01-04|  3|44.4|17179869184|
|    b|2019-01-01|  1|11.1|25769803776|
|    b|2019-01-02|  2|22.2|34359738368|
|    b|2019-01-03|  3|33.3|42949672960|
|    b|2019-01-05|  5|55.5|51539607552|
+-----+----------+---+----+-----------+
```

We put the ```by```, ```on``` and ```id``` columns in a separate frame:

```
right_temp.show()
+-----+-----------+----------+
|group|         id|  datetime|
+-----+-----------+----------+
|    a|          0|2019-01-01|
|    a| 8589934592|2019-01-02|
|    a|17179869184|2019-01-04|
|    b|25769803776|2019-01-01|
|    b|34359738368|2019-01-02|
|    b|42949672960|2019-01-03|
|    b|51539607552|2019-01-05|
+-----+-----------+----------+
```

Then we add create a union of ```left``` and ```right_temp``` dataframes, fill with null where the field is missing:


```
union.show()
+-----+----------+----+------+-----------+
|group|  datetime|   X|animal|         id|
+-----+----------+----+------+-----------+
|    a|2019-01-01|   1|   cat|       null|
|    a|2019-01-02|   2|   cat|       null|
|    a|2019-01-03|   3| horse|       null|
|    b|2019-01-01|   1|   dog|       null|
|    b|2019-01-02|   2|   cat|       null|
|    b|2019-01-04|   4| mouse|       null|
|    a|2019-01-01|null|  null|          0|
|    a|2019-01-02|null|  null| 8589934592|
|    a|2019-01-04|null|  null|17179869184|
|    b|2019-01-01|null|  null|25769803776|
|    b|2019-01-02|null|  null|34359738368|
|    b|2019-01-03|null|  null|42949672960|
|    b|2019-01-05|null|  null|51539607552|
+-----+----------+----+------+-----------+
```

Sort such that two rows have the same ```on``` key, the right row comes after the left row:
```
+-----+----------+----+------+-----------+
|group|  datetime|   X|animal|         id|
+-----+----------+----+------+-----------+
|    a|2019-01-01|   1|   cat|       null|
|    a|2019-01-01|null|  null|          0|
|    a|2019-01-02|   2|   cat|       null|
|    a|2019-01-02|null|  null| 8589934592|
|    a|2019-01-03|   3| horse|       null|
|    a|2019-01-04|null|  null|17179869184|
|    b|2019-01-01|   1|   dog|       null|
|    b|2019-01-01|null|  null|25769803776|
|    b|2019-01-02|   2|   cat|       null|
|    b|2019-01-02|null|  null|34359738368|
|    b|2019-01-03|null|  null|42949672960|
|    b|2019-01-04|   4| mouse|       null|
|    b|2019-01-05|null|  null|51539607552|
+-----+----------+----+------+-----------+

```

For each group in the union, the id is replaced by the last non-null id using windowing:

```
un
union.show()
+-----+----------+----+------+-----------+
|group|  datetime|   X|animal|         id|
+-----+----------+----+------+-----------+
|    b|2019-01-01|   1|   dog|       null|
|    b|2019-01-01|null|  null|       null|
|    b|2019-01-02|   2|   cat|25769803776|
|    b|2019-01-02|null|  null|25769803776|
|    b|2019-01-03|null|  null|34359738368|
|    b|2019-01-04|   4| mouse|42949672960|
|    b|2019-01-05|null|  null|42949672960|
|    a|2019-01-01|   1|   cat|       null|
|    a|2019-01-01|null|  null|       null|
|    a|2019-01-02|   2|   cat|          0|
|    a|2019-01-02|null|  null|          0|
|    a|2019-01-03|   3| horse| 8589934592|
|    a|2019-01-04|null|  null| 8589934592|
+-----+----------+----+------+-----------+
```

All rows where ```X``` is Null are dropped:
```
+-----+----------+---+------+-----------+
|group|  datetime|  X|animal|         id|
+-----+----------+---+------+-----------+
|    b|2019-01-01|  1|   dog|       null|
|    b|2019-01-02|  2|   cat|25769803776|
|    b|2019-01-04|  4| mouse|42949672960|
|    a|2019-01-01|  1|   cat|       null|
|    a|2019-01-02|  2|   cat|          0|
|    a|2019-01-03|  3| horse| 8589934592|
+-----+----------+---+------+-----------+
```
Now all that is left to do, is joining the rest of the columns of the right dataframe. This can be easily done using the generated ```id```: 

```
+-----------+-----+----------+---+------+----+----+
|         id|group|  datetime|  X|animal|   Z|   Y|
+-----------+-----+----------+---+------+----+----+
| 8589934592|    a|2019-01-03|  3| horse|   2|22.2|
|          0|    a|2019-01-02|  2|   cat|   1|11.1|
|       null|    b|2019-01-01|  1|   dog|null|null|
|       null|    a|2019-01-01|  1|   cat|null|null|
|42949672960|    b|2019-01-04|  4| mouse|   3|33.3|
|25769803776|    b|2019-01-02|  2|   cat|   1|11.1|
+-----------+-----+----------+---+------+----+----+
```

Finally, drop the ```id``` column:

```
+-----+----------+---+------+----+----+
|group|  datetime|  X|animal|   Z|   Y|
+-----+----------+---+------+----+----+
|    a|2019-01-03|  3| horse|   2|22.2|
|    a|2019-01-02|  2|   cat|   1|11.1|
|    b|2019-01-01|  1|   dog|null|null|
|    a|2019-01-01|  1|   cat|null|null|
|    b|2019-01-04|  4| mouse|   3|33.3|
|    b|2019-01-02|  2|   cat|   1|11.1|
+-----+----------+---+------+----+----+
```
