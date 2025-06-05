from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

FEATURE_SCHEMA = StructType([
    StructField("header", StringType(), False),
    StructField("window_id", StringType(), False),
    StructField("sequence", StringType(), False),
    StructField("gc_content", DoubleType(), False),
    StructField("lowercase_pct", DoubleType(), False),
    StructField("longest_run", IntegerType(), False),
    StructField("label", IntegerType(), False),
])


FASTA_SCHEMA = StructType([
    StructField("header", StringType(), False),
    StructField("sequence", StringType(), False),
])