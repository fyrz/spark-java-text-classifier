#!/bin/bash

script_dir="$(dirname "$0")"
"$script_dir/validate_env.sh"
if [[ $? -ne 0 ]]; then
  exit 1
fi

# read properties
. "$script_dir/config.properties"

# Use spark-submit to run your application
$SPARK_HOME/bin/spark-submit \
	--class "org.fyrz.textclassifier.NaiveBayesEvaluationApplication" \
	--master local[$CONFIG_CONCURRENCY] \
	--executor-memory $CONFIG_EXECUTOR_MEMORY \
	--driver-memory $CONFIG_DRIVER_MEMORY \
	build/libs/spark-java-text-classifier-$CONFIG_CLASSFIER_VERSION-all.jar

