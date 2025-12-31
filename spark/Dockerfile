# syntax=docker/dockerfile:1

FROM apache/spark:3.5.1-scala2.12-java11-ubuntu

WORKDIR /opt/spark-app

# Copy only spark app assets
COPY spark-app/ /opt/spark-app/

# Default: keep container alive or override with spark-submit in compose
CMD ["bash", "-lc", "sleep infinity"]
