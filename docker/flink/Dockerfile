FROM flink:1.18.1-scala_2.12-java11

RUN apt-get update -y && \
    apt-get install -y \
        python3 \
        python3-pip \
        python3-dev \
        zip \
    && rm -rf /var/lib/apt/lists/*

RUN ln -sf /usr/bin/python3 /usr/bin/python

RUN pip3 install \
    apache-flink==1.18.0 \
    python-dotenv

# Copy connector jars
COPY docker/flink/jars/flink-sql-connector-kafka-3.2.0-1.18.jar /opt/flink/lib/

COPY docker/flink/jars/flink-json-1.18.1.jar /opt/flink/lib/

# IMPORTANT
COPY . /opt/project

ENV PYTHONPATH=/opt/project
ENV PYFLINK_PYTHON=/usr/bin/python3