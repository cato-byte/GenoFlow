import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, lower, regexp_replace, when, udf
from pyspark.sql.types import IntegerType, DoubleType

def create_spark_session(app_name="FeatureEngineeringPipeline") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()

def compute_gc_content(sequence: str) -> float:
    if not sequence:
        return 0.0
    gc = sequence.count('G') + sequence.count('C')
    return round(gc / len(sequence), 4)

def count_base(base: str):
    return udf(lambda seq: seq.upper().count(base), IntegerType())

def process_feature_engineering(spark: SparkSession, input_path: str, output_train_path: str, output_test_path: str, logger) -> None:
    logger.info("🔍 Reading cleaned FASTA data...")
    df = spark.read.parquet(input_path)

    logger.info("🧼 Cleaning and normalizing sequences...")
    df = df.withColumn("sequence", lower(regexp_replace("sequence", r"[^atcg]", "")))

    logger.info("🔢 Calculating base counts and features...")
    df = df.withColumn("length", length(col("sequence"))) \
           .withColumn("a_count", count_base("A")(col("sequence"))) \
           .withColumn("t_count", count_base("T")(col("sequence"))) \
           .withColumn("c_count", count_base("C")(col("sequence"))) \
           .withColumn("g_count", count_base("G")(col("sequence")))

    gc_content_udf = udf(compute_gc_content, DoubleType())
    df = df.withColumn("gc_content", gc_content_udf(col("sequence")))

    logger.info("🏷️ Labeling sequences based on GC content...")
    df = df.withColumn("label", when(col("gc_content") > 0.5, 1).otherwise(0))

    df = df.select("length", "a_count", "t_count", "c_count", "g_count", "gc_content", "label")

    logger.info("✂️ Splitting into train/test sets...")
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    logger.info("💾 Writing output datasets to Parquet...")
    train_df.write.mode("overwrite").parquet(output_train_path)
    test_df.write.mode("overwrite").parquet(output_test_path)

    logger.info("✅ Feature engineering completed successfully.")

    logger.info("💡 Sample output:")
    df.show(5, truncate=False)

    logger.info("🧠 Spark plan:")
    df.explain(mode="formatted")

def main():
    spark = create_spark_session()
    logger = logging.getLogger("Feature Engineering")
    logging.basicConfig(level=logging.INFO)

    input_path = "s3a://processed/cleaned_sequences.parquet"
    output_train_path = "s3a://processed/features_train.parquet"
    output_test_path = "s3a://processed/features_test.parquet"

    process_feature_engineering(spark, input_path, output_train_path, output_test_path, logger)

    logger.info("🛑 Stopping Spark session.")
    spark.stop()

if __name__ == "__main__":
    main()                                                          