# Floci EMR compute clone

This Java service adds a Docker backed Spark data plane to Floci's EMR control plane. Floci remains the source of cluster metadata. Because Floci currently marks a step complete when it is accepted, AWS clients submit EMR calls to this service on port `4567`; it forwards those calls to Floci on port `4566` and overlays execution status from `data/steps.json`.

## Project layout

```text
pom.xml                              Maven build and executable JAR
src/main/java/io/floci/emrclone/     Java application code
  Main.java                          Configuration and process entry point
  api/EmrProxy.java                  EMR API forwarding and status overlay
  control/Controller.java            Cluster and step reconciliation
  floci/FlociClient.java             Floci EMR and S3 HTTP client
  runtime/DockerRuntime.java         Primary/core containers and spark-submit
  state/StepStore.java               Durable local step state
data/                                Runtime state and step logs (ignored by Git)
```

## Run locally

Requirements: Java 17 or later, Maven, Docker CLI, a running Docker daemon, and Floci reachable at `http://localhost:4566`.

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

This creates one primary and two core nodes, then runs 100 SparkPi partitions with one executor core per node:

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
  --steps 'Type=CUSTOM_JAR,Name=SparkPiTwoWorkers,Jar=command-runner.jar,Args=[spark-submit,--class,org.apache.spark.examples.SparkPi,--conf,spark.executor.cores=1,--conf,spark.cores.max=2,/opt/spark/examples/jars/spark-examples_2.12-3.5.7.jar,100],ActionOnFailure=CONTINUE' \
  --endpoint-url http://127.0.0.1:4567 \
  --query 'StepIds[0]' --output text)

aws emr describe-step \
  --cluster-id "$CLUSTER_ID" --step-id "$STEP_ID" \
  --endpoint-url http://127.0.0.1:4567 \
  --query 'Step.Status.State' --output text
```

The primary's Spark master UI lists the live workers. Find its host port with:

```sh
docker port "emr-$(printf '%s' "$CLUSTER_ID" | tr '[:upper:]' '[:lower:]')-primary" 8080/tcp
```

Open the returned `localhost:<port>` address and confirm both core workers are `ALIVE`. The step log is saved under `data/logs/<cluster-id>/<step-id>.log`; its executor registration lines include each worker's Docker network address.

Configuration uses environment variables: `FLOCI_ENDPOINT`, `FLOCI_CONTAINER`, `EMR_PROXY_HOST`, `EMR_PROXY_PORT`, `EMR_SPARK_IMAGE`, `EMR_DOCKER_NETWORK`, and `EMR_DATA_DIR`.

## Current execution scope

The node containers use the pinned Apache Spark 3.5.7 image. This first runtime uses Spark standalone scheduling across the primary and core containers. Each core worker advertises one executor core, and the primary publishes the Spark master UI on a Docker assigned host port. Find that port with `docker port <primary-container> 8080/tcp`. It accepts Spark steps represented as `command-runner.jar` with `spark-submit`, or a JAR step. Other EMR step types and full EMR release behavior are not implemented yet.
