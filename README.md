#  GenoFlow

** GenoFlow** is a modular, multi-container data pipeline designed for analyzing genomic regions based on GC content and repeat density. It leverages real DNA sequence data to preprocess, train, and predict genomic features using machine learning techniques.

---

## 🧬 Overview

 GenoFlow is built to process genomic data:
- Analyze DNA sequences by chromosome or contig.
- Chunk sequences into fixed-length windows for statistical analysis.
- Train machine learning models to classify genomic regions based on GC content and repeat density.

The pipeline is modular and containerized, making it easy to deploy and scale across different environments.

---

## 🔧 Features

- **FASTA File Processing**: Handles raw genomic data in FASTA format.
- **Window-Based Analysis**: Splits sequences into fixed-length windows for feature extraction.
- **GC Content Calculation**: Computes GC content and other statistics for each window.
- **Machine Learning Integration**: Trains models using scikit-learn and predicts genomic features.
- **Orchestration with Airflow**: Optional DAG orchestration for ETL, training, and prediction workflows.
- **S3-Compatible Storage**: Uses MinIO for storing raw and processed data.

---

## 🏗️ Architecture

| Component        | Description |
|------------------|-------------|
| **MinIO**        | Local S3-compatible object store with `raw/` and `processed/` buckets |
| **Spark Preprocess** | PySpark-based job that chunks genome FASTA files, calculates features, and writes structured data to MinIO |
| **DS Model**     | NumPy + scikit-learn pipeline that trains a model from processed data and makes predictions |
| **Airflow (optional)** | DAG orchestration for sequencing jobs (ETL → Train → Predict) |
| **Terraform (optional)** | Infrastructure as Code mock-up for AWS setup (S3, EC2) |

---

## 💾 Data Flow

1. **Upload FASTA**: Place raw genomic data into the `raw/` bucket in MinIO.
2. **Preprocessing**: The Spark job chunks the genome into windows, calculates features, and stores structured data in the `processed/` bucket.
3. **Model Training**: The DS Model container loads processed data, trains a machine learning model, and makes predictions.
4. **Output**: Results are saved locally or pushed to a reporting bucket.

---

## 🚀 Getting Started

### Prerequisites
- Docker and Docker Compose installed.
- Python 3.9+ for local development (optional).
- FASTA files for genomic data.

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/gc_predictor.git
   cd gc_predictor
   ```
2. Copy the example environment file:
   ```bash
   cp .env.example .env
   ```
3. Build and start the Docker containers:
   ```bash
   docker compose up --build
   ```

### Usage
- Place your FASTA files in the `raw/` directory in MinIO.
- Run the preprocessing Spark job.
- Train the DS Model with the processed data.
- (Optional) Set up Airflow for automated workflows.

### Note on FASTA Format

FASTA is a text-based format used to represent nucleotide or amino acid sequences.

It has a very simple structure:

>sequence_id_or_description
ACTGACTGACTGACTGACTG
ACTGACTGACTG
>another_sequence
TTCGGTAACTAGTCGGTAC

 Each entry is:
A header line (starts with >)

Contains the sequence name (e.g., >chr1 or >contig12345)

May include metadata (e.g., organism, gene, length, etc.)

Followed by one or more lines of sequence (ATCG...)

❓ Why Is the Header Important?
It helps you:

Know which chromosome or contig the sequence belongs to

Identify and process one chromosome at a time

Avoid mixing different sequences (especially useful if you're doing ML by chromosome)

🔲 What’s a Window?

In bioinformatics:

A window is a fixed-length chunk of a sequence.

Example:

You have a DNA sequence of 20,000 bp (bases)

You use a window size of 1,000

You get 20 rows (0–999, 1000–1999, ...)

Each window is treated like a sample for ML — you compute stats like GC content and label it based on soft-masking.

The test data used fot this mvp is the tibetan frog genome obtained from 

https://hgdownload.soe.ucsc.edu/goldenPath/nanPar1/bigZips/

instrictions to downlaod and extract the dataset. 

wget --timestamping \
  'ftp://hgdownload.cse.ucsc.edu/goldenPath/nanPar1/bigZips/nanPar1.fa.gz' \
  -O nanPar1.fa.gz

gunzip nanPar1.fa.gz

