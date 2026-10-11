FROM apache/spark:3.5.7-python3

ARG HADOOP_VERSION=3.3.6
USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
        -o /tmp/hadoop.tar.gz \
    && mkdir -p /opt/hadoop \
    && tar -xzf /tmp/hadoop.tar.gz --strip-components=1 -C /opt/hadoop \
    && rm /tmp/hadoop.tar.gz \
    && mkdir -p /opt/emr/bin /opt/hadoop-data/hdfs/name /opt/hadoop-data/hdfs/data \
        /opt/hadoop-data/yarn/local /opt/hadoop-data/yarn/logs /opt/hadoop/logs \
    && chown -R 185:0 /opt/hadoop /opt/hadoop-data /opt/emr

ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    HADOOP_LOG_DIR=/opt/hadoop/logs \
    SPARK_HOME=/opt/spark \
    PATH=/opt/hadoop/bin:/opt/hadoop/sbin:/opt/spark/bin:${PATH}

COPY --chown=185:0 start-node.sh /opt/emr/bin/start-node.sh
RUN chmod 0755 /opt/emr/bin/start-node.sh

USER 185
ENTRYPOINT ["/opt/emr/bin/start-node.sh"]
