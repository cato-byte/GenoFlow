import pytest
from pyspark.sql import SparkSession
from spark_preprocess.run_fasta_clean import process_fasta_lines


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[2]").appName("TestFASTAProcessing").getOrCreate()


def test_process_fasta_lines_basic(spark):
    input_lines = [
        ">seq1",
        "ATCGatcg",
        "GGGCCC",
        ">seq2",
        "TTTAAA",
        "ggggcc"
    ]

    df = process_fasta_lines(spark, input_lines)
    result = df.collect()

    assert len(result) == 2

    assert result[0]["header"] == "seq1"
    assert result[0]["sequence"] == "ATCGatcgGGGCCC"
    assert result[1]["header"] == "seq2"
    assert result[1]["sequence"] == "TTTAAAggggcc"


def test_process_fasta_lines_empty(spark):
    df = process_fasta_lines(spark, [])
    assert df.count() == 0


def test_single_header_multiple_lines(spark):
    input_lines = [
        ">frog_chr1",
        "ACGTACGTACGT",
        "ttggcc",
        "AAAGGG"
    ]
    df = process_fasta_lines(spark, input_lines)
    row = df.first()

    assert df.count() == 1
    assert row["header"] == "frog_chr1"
    assert row["sequence"] == "ACGTACGTACGTttggccAAAGGG"


def test_missing_header(spark):
    input_lines = [
        "ACGTACGT",
        "TTGGCC"
    ]
    df = process_fasta_lines(spark, input_lines)
    # This input is invalid; it should not produce a result
    assert df.count() == 0


def test_malformed_headers(spark):
    input_lines = [
        ">",
        "ACGT",
        ">chr2",
        "TTGGCC"
    ]
    df = process_fasta_lines(spark, input_lines)
    result = df.collect()

    assert len(result) == 2
    assert result[0]["header"] == ""  # malformed header
    assert result[0]["sequence"] == "ACGT"
    assert result[1]["header"] == "chr2"
    assert result[1]["sequence"] == "TTGGCC"


def test_schema_and_dtypes(spark):
    input_lines = [
        ">header1",
        "ACGTACGT"
    ]
    df = process_fasta_lines(spark, input_lines)

    fields = df.schema.fields
    assert len(fields) == 2
    assert fields[0].name == "header"
    assert fields[0].dataType.simpleString() == "string"
    assert fields[1].name == "sequence"
    assert fields[1].dataType.simpleString() == "string"
