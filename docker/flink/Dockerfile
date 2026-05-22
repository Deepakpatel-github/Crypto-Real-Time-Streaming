# flink:1.18-python3 tag does not exist — Python must be installed manually
# Use the correct official tag format: flink:VERSION-scala_2.12-javaVERSION
FROM flink:1.18.1-scala_2.12-java11

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
        curl \
        netcat-openbsd \
        python3 \
        python3-dev \
        python3-pip \
        wget && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

RUN wget -q -P /opt/flink/lib \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

RUN pip3 install --no-cache-dir \
    apache-flink==1.18.1 \
    clickhouse-driver==0.2.7 \
    kafka-python==2.0.2 \
    python-dotenv==1.0.0 \
    pyyaml==6.0.1

WORKDIR /opt/flink/jobs
