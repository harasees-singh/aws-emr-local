#!/usr/bin/env bash
set -euo pipefail

: "${NODE_ROLE:?NODE_ROLE must be primary or core}"
: "${NODE_HOST:?NODE_HOST must be set}"
: "${YARN_RM_HOST:?YARN_RM_HOST must be set}"
: "${HDFS_NAMENODE_HOST:?HDFS_NAMENODE_HOST must be set}"

if [[ -z "${JAVA_HOME:-}" ]]; then
  JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")"
fi
export JAVA_HOME
export HADOOP_COMMON_HOME="$HADOOP_HOME"
export HADOOP_HDFS_HOME="$HADOOP_HOME"
export HADOOP_YARN_HOME="$HADOOP_HOME"
export HADOOP_MAPRED_HOME="$HADOOP_HOME"
export YARN_LOG_DIR="$HADOOP_LOG_DIR"

mkdir -p "$HADOOP_LOG_DIR" /opt/hadoop-data/hdfs/name /opt/hadoop-data/hdfs/data \
    /opt/hadoop-data/yarn/local /opt/hadoop-data/yarn/logs

cat > "$HADOOP_CONF_DIR/core-site.xml" <<EOF
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://${HDFS_NAMENODE_HOST}:9000</value></property>
  <property><name>hadoop.tmp.dir</name><value>/opt/hadoop-data/tmp</value></property>
</configuration>
EOF

cat > "$HADOOP_CONF_DIR/hdfs-site.xml" <<EOF
<configuration>
  <property><name>dfs.namenode.rpc-address</name><value>0.0.0.0:9000</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file:///opt/hadoop-data/hdfs/name</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file:///opt/hadoop-data/hdfs/data</value></property>
  <property><name>dfs.replication</name><value>1</value></property>
  <property><name>dfs.datanode.hostname</name><value>${NODE_HOST}</value></property>
  <property><name>dfs.client.use.datanode.hostname</name><value>true</value></property>
</configuration>
EOF

cat > "$HADOOP_CONF_DIR/yarn-site.xml" <<EOF
<configuration>
  <property><name>yarn.resourcemanager.hostname</name><value>${YARN_RM_HOST}</value></property>
  <property><name>yarn.resourcemanager.webapp.address</name><value>${YARN_RM_HOST}:8088</value></property>
  <property><name>yarn.nodemanager.hostname</name><value>${NODE_HOST}</value></property>
  <property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>
  <property><name>yarn.nodemanager.local-dirs</name><value>/opt/hadoop-data/yarn/local</value></property>
  <property><name>yarn.nodemanager.log-dirs</name><value>/opt/hadoop-data/yarn/logs</value></property>
  <property><name>yarn.nodemanager.resource.memory-mb</name><value>2048</value></property>
  <property><name>yarn.nodemanager.resource.cpu-vcores</name><value>2</value></property>
  <property><name>yarn.nodemanager.vmem-check-enabled</name><value>false</value></property>
  <property><name>yarn.scheduler.minimum-allocation-mb</name><value>256</value></property>
  <property><name>yarn.scheduler.maximum-allocation-mb</name><value>2048</value></property>
  <property><name>yarn.scheduler.minimum-allocation-vcores</name><value>1</value></property>
  <property><name>yarn.scheduler.maximum-allocation-vcores</name><value>2</value></property>
</configuration>
EOF

cat > "$HADOOP_CONF_DIR/mapred-site.xml" <<EOF
<configuration>
  <property><name>mapreduce.framework.name</name><value>yarn</value></property>
</configuration>
EOF

wait_for_port() {
  local host="$1" port="$2" attempts="${3:-120}"
  for ((attempt = 0; attempt < attempts; attempt++)); do
    if bash -c "echo > /dev/tcp/${host}/${port}" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "Timed out waiting for ${host}:${port}" >&2
  return 1
}

start_daemon() {
  local component="$1" daemon="$2"
  "$component" --daemon start "$daemon"
}

stop_daemons() {
  case "$NODE_ROLE" in
    primary)
      yarn --daemon stop resourcemanager || true
      hdfs --daemon stop datanode || true
      hdfs --daemon stop namenode || true
      ;;
    core)
      yarn --daemon stop nodemanager || true
      hdfs --daemon stop datanode || true
      ;;
  esac
}
trap 'stop_daemons; exit 0' TERM INT

if [[ "$NODE_ROLE" == "primary" ]]; then
  if [[ ! -f /opt/hadoop-data/hdfs/name/current/VERSION ]]; then
    hdfs namenode -format -nonInteractive
  fi
  start_daemon hdfs namenode
  start_daemon hdfs datanode
  wait_for_port "$HDFS_NAMENODE_HOST" 9000
  hdfs dfsadmin -safemode wait
  hdfs dfs -mkdir -p /tmp /user/spark
  hdfs dfs -chmod 1777 /tmp
  start_daemon yarn resourcemanager
  wait_for_port "$YARN_RM_HOST" 8032
elif [[ "$NODE_ROLE" == "core" ]]; then
  wait_for_port "$HDFS_NAMENODE_HOST" 9000
  wait_for_port "$YARN_RM_HOST" 8032
  start_daemon hdfs datanode
  start_daemon yarn nodemanager
else
  echo "Unsupported NODE_ROLE: $NODE_ROLE" >&2
  exit 2
fi

while true; do sleep 3600 & wait $!; done
