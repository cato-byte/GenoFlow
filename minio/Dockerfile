FROM minio/mc

WORKDIR /app
COPY create_buckets.sh .
RUN chmod +x create_buckets.sh

ENTRYPOINT ["/app/create_buckets.sh"]
