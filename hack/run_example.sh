#!/usr/bin/env bash

JAR_PATH="../target/scala-2.12/spark-denormalize-assembly_0.1.0-SNAPSHOT_spark-3.0.0_scala-2.12.jar"

# environment specific configs
SPARK_SUBMIT_BINARY="$HOME/opt/spark-3.0.1-bin-hadoop2.7/bin/spark-submit"
SPARK_JARS_FOLDER="$HOME/opt/spark_jars"
GCLOUD_APP_DEFAULT_CREDENTIALS="$HOME/.config/gcloud/application_default_credentials.json"

chmod +x $JAR_PATH

$SPARK_SUBMIT_BINARY \
  --conf "spark.jars=$SPARK_JARS_FOLDER/gcs-connector-hadoop2-latest.jar" \
  --conf "spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem" \
  --conf "spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS" \
  --conf "spark.hadoop.fs.gs.auth.service.account.enable=true" \
  --conf "spark.hadoop.fs.gs.auth.service.account.json.keyfile=$GCLOUD_APP_DEFAULT_CREDENTIALS" \
  $JAR_PATH \
  --config \
    "../docs/yaml/data_sources/hdfslike.yaml" \
    "../docs/yaml/data_sources/jdbc.yaml" \
    "../docs/yaml/data_sources/virtual_complex.yaml" \
    "../docs/yaml/data_sources/virtual_simple.yaml" \
    "../docs/yaml/data_relationship.yaml" \
    "../docs/yaml/metadata_applicator.yaml" \
    "../docs/yaml/output_dataset.yaml"
