
# Read options

## schema_hint

Hard-coded [column type](../../../../../user-guide/storage/data-types.md). Useful when a column is of type `any` (a composite data type serialized as `yson`).
The value will be deserialized as the specified type.

Python example:
```python
spark.read.schema_hint({"value": MapType(StringType(), LongType())}).yt("//sys/spark/examples/example_yson")
```

Scala example:
```scala
df.write
    .schemaHint(Map("a" ->
        YtLogicalType.VariantOverTuple(Seq(
          (YtLogicalType.String, Metadata.empty), (YtLogicalType.Double, Metadata.empty)))))
    .yt(tmpPath)
```

## transaction

Reading from a [transaction](../../../../../user-guide/storage/transactions.md). For more details, see [Reading and writing within a transaction](../read-transaction.md).

Scala example:

```scala
val transaction = YtWrapper.createTransaction(None, 10 minute)
df.write.transaction(transaction.getId.toString).yt(tmpPath)
transaction.commit().get(10, TimeUnit.SECONDS)
```

## Schema v3

Read tables with schema in [type_v3](../../../../../user-guide/storage/data-types.md) instead of type_v1.
Setup in [Spark configuration](../cluster/configuration.md) or write option.

Python example:
```python
spark.read.option("parsing_type_v3", "true").yt("//sys/spark/examples/example_yson")
```
