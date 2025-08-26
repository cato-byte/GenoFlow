## 🧬 Overview

 GenoFlow is built to process genomic data:
- Analyze DNA sequences by chromosome or contig.
- Chunk sequences into fixed-length windows for statistical analysis.
- Train machine learning models to classify genomic regions based on GC content and repeat density.


## MinIO How it works today

MinIO itself is just a container running with its own persistent volume. That means all the objects I upload live in a Docker volume called miniogc_data, so if I restart the container the data is still there. MinIO is only responsible for storage, nothing else.

I have a small “init job” called miniogc-init. It uses the official MinIO client (mc) and runs a shell script (infra/minio/init.sh). That script retries until MinIO is actually ready, then creates the buckets I need. It’s a one-shot job: it runs, sets things up, and exits. 

For loading data into MinIO I decided to do it in code, not in Docker. 






