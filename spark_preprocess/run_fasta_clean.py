from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import monotonically_increasing_id, col, sum,  first, collect_list, concat_ws,when,expr
from pyspark.sql.types import DataFrame

import logging

def create_spark_session():
    return SparkSession.builder \
        .appName("FASTA Cleaner") \
        .master("local[*]") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

def process_fasta_lines(df_raw: DataFrame) -> DataFrame:
    

    # Flag headers (lines starting with >)
    df = df_raw.withColumnRenamed("value", "line") \
               .withColumn("is_header", expr("startswith(line, '>')").cast("int")) \
               .withColumn("line_id", monotonically_increasing_id())

    # Assign group ID per header (chromosome)
    w = Window.orderBy("line_id")
    df = df.withColumn("group_id", sum("is_header").over(w))

    # Collect header and sequences per group
    grouped = df.groupBy("group_id").agg(
        first(F.when(col("is_header") == 1, col("line"))).alias("header"),
        concat_ws("", collect_list(when(col("is_header") == 0, col("line")))).alias("sequence")
    ).filter("sequence IS NOT NULL")

    return grouped.select("header", "sequence")

def main():
    spark = create_spark_session()
    logger = logging.getLogger("FASTA Cleaner")
    logging.basicConfig(level=logging.INFO)

    # Load FASTA file from MinIO
    logger.info("Reading FASTA file from MinIO...")
    df_raw = spark.read.text("s3a://raw/nanPar1.fa")

    # Process into (header, sequence) table
    logger.info("Processing FASTA into (header, sequence)...")
    df_cleaned = process_fasta_lines(df_raw)

    logger.info(f"Writing cleaned sequences to Parquet...")

    
    df_cleaned.write.mode("overwrite").partitionBy("header_prefix").parquet("s3a://processed/cleaned_sequences.parquet")

    # Show basic metrics
    logger.info(" Sample output:")
    df_cleaned.show(5, truncate=False)

    logger.info(" Spark plan:")
    df_cleaned.explain(mode="formatted")

    logger.info(" Done.")

    spark.stop()

if __name__ == "__main__":
    main()