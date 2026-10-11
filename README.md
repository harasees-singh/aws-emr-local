# Floci EMR compute clone

This Java service adds a Docker backed Spark data plane to Floci's EMR control plane. Floci remains the source of cluster metadata. Because Floci currently marks a step complete when it is accepted, AWS clients submit EMR calls to this service on port `4567`; it forwards those calls to Floci on port `4566` and overlays execution status from `data/steps.json`.

## Project layout

```text
pom.xml                              Maven build and executable JAR
src/main/java/io/floci/emrclone/     Java application code
  AppConfig.java                      Validated environment configuration
  Main.java                          Configuration and process entry point
  api/EmrProxy.java                  EMR API forwarding and status overlay
  control/Controller.java            Cluster and step reconciliation
  floci/FlociClient.java             Floci EMR and S3 HTTP client
  runtime/DockerRuntime.java         YARN/HDFS containers and spark-submit
  runtime/DockerCommand.java         Bounded Docker process execution
  runtime/SparkUiMonitor.java        Executor and task placement from Spark UI
  state/StepStore.java               Durable local step state
data/                                Runtime state and step logs (ignored by Git)
docker/yarn/                         Spark, Hadoop, and YARN runtime image
```

## Run locally

Requirements: Java 17 or later, Maven, Docker CLI, a running Docker daemon, and Floci reachable at `http://localhost:4566`.

Build the runtime image used for YARN ResourceManager, NodeManager, HDFS, and Spark client processes:

```sh
docker build -f docker/yarn/Dockerfile -t emr-spark-yarn:3.5.7-hadoop3.3.6 docker/yarn
```

```sh
mvn package
java -jar target/emr-compute-clone-0.1.0-SNAPSHOT.jar
```

The service listens on `http://127.0.0.1:4567`. Use it as the endpoint for EMR commands so its step status reflects the Spark process:

```sh
aws emr list-clusters --endpoint-url http://127.0.0.1:4567
```

Other AWS services, including S3, should continue to use Floci at `http://localhost:4566`.

## Run Spark across two core nodes

This creates one primary and two core nodes, then runs SparkPi with one executor on each YARN NodeManager. Executors default to 512 MB of heap plus 384 MB of overhead so two executors fit alongside the YARN ApplicationMaster on 2 GB nodes. Spark waits up to 60 seconds for all requested executors to register before scheduling tasks, giving short jobs time to use both nodes:

```sh
CLUSTER_ID=$(aws emr create-cluster \
  --name spark-two-core-demo \
  --release-label emr-7.0.0 \
  --applications Name=Spark \
  --instance-groups \
    InstanceGroupType=MASTER,InstanceType=m5.xlarge,InstanceCount=1 \
    InstanceGroupType=CORE,InstanceType=m5.xlarge,InstanceCount=2 \
  --use-default-roles \
  --endpoint-url http://127.0.0.1:4567 \
  --query ClusterId --output text)

STEP_ID=$(aws emr add-steps \
  --cluster-id "$CLUSTER_ID" \
  --steps 'Type=CUSTOM_JAR,Name=SparkPiYarn,Jar=command-runner.jar,Args=[spark-submit,--class,org.apache.spark.examples.SparkPi,--executor-cores,1,--num-executors,2,/opt/spark/examples/jars/spark-examples_2.12-3.5.7.jar,100],ActionOnFailure=CONTINUE' \
  --endpoint-url http://127.0.0.1:4567 \
  --query 'StepIds[0]' --output text)

aws emr describe-step \
  --cluster-id "$CLUSTER_ID" --step-id "$STEP_ID" \
  --endpoint-url http://127.0.0.1:4567 \
  --query 'Step.Status.State' --output text
```

The primary runs the YARN ResourceManager and HDFS NameNode. Core nodes run YARN NodeManagers and HDFS DataNodes. Find the ResourceManager UI port with:

```sh
docker port "emr-$(printf '%s' "$CLUSTER_ID" | tr '[:upper:]' '[:lower:]')-primary" 8088/tcp
```

Open the returned `localhost:<port>` address and confirm both core nodes are `RUNNING`. Spark submits in YARN client mode: its driver stays on the primary while YARN places executors on core nodes. The step log is saved under `data/logs/<cluster-id>/<step-id>.log`.

The Java service also writes an event stream to its standard output. Key events include:

```text
yarn.resourcemanager.ready ...
yarn.nodemanager.registered ... host=... vcores=2 memoryMb=2048 ...
yarn.resources.visible ... registeredNodes=2 ...
emr.step.started ...
spark.application.registered ...
spark.executor.registered ... host=... cpuCores=1 yarnNodeVcores=2 yarnNodeMemoryMb=2048 storageMemoryAvailableBytes=...
spark.task ... partition=... executor=... host=... status=SUCCESS
spark.eventlog.replayed ... taskEndEvents=100
spark.application.task-summary ... tasksByHost={core-1=50, core-2=50} completedTasksObserved=100
emr.step.finished ... state=COMPLETED exitCode=0 ...
```

These records show YARN's registered node inventory, executor placement, and the host that ran each task partition. The executor event includes its CPU slots, its YARN node's advertised core and memory capacity, and Spark storage-memory usage and capacity. Storage memory is the portion Spark uses for cached data; it is not the executor JVM heap size. Spark UI polling provides live updates; after the application exits, the service replays Spark's event log to fill in any task events that happened between polls. Service events are written both to the console and to rotating `data/emr-service-*.log` files. Spark's driver and executor output remains in `data/logs/<cluster-id>/<step-id>.log`.

Configuration uses environment variables: `FLOCI_ENDPOINT`, `FLOCI_CONTAINER`, `EMR_PROXY_HOST`, `EMR_PROXY_PORT`, `EMR_SPARK_IMAGE`, `EMR_DOCKER_NETWORK`, and `EMR_DATA_DIR`.

## Current execution scope

The runtime image pins Apache Spark 3.5.7 and Hadoop 3.3.6. It runs a single YARN ResourceManager on the primary, with one NodeManager on each core node. HDFS provides the shared staging area YARN needs for Spark's runtime and application files; this local development cluster uses replication factor one. Spark steps use YARN client deploy mode so the existing Spark UI and event-log monitoring remain available on the primary. NodeManagers currently advertise two virtual cores and 2 GB of memory each. Existing containers from the standalone runtime cannot be upgraded in place; terminate and recreate those clusters after switching to this runtime. The service accepts `command-runner.jar` steps with `spark-submit`, or a JAR step. Other EMR step types and full EMR release behavior are not implemented yet.
