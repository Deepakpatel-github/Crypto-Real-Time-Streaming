# flink:1.18-python3 tag does not exist — Python must be installed manually
# Use the correct official tag format: flink:VERSION-scala_2.12-javaVERSION
FROM flink:1.18.1-scala_2.12-java11

# Install Python 3 + pip
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-dev && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Install PyFlink and job dependencies
RUN pip3 install --no-cache-dir \
    apache-flink==1.18.1 \
    kafka-python==2.0.2 \
    clickhouse-driver==0.2.7 \
    pyyaml==6.0.1 \
    python-dotenv==1.0.0

WORKDIR /opt/flink/jobs