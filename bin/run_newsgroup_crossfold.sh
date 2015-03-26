#!/bin/bash

script_dir="$(dirname "$0")"
"$script_dir/validate_env.sh"
if [[ $status -ne 0 ]]; then
  exit 1
fi

# Use spark-submit to run your application
$SPARK_HOME/bin/spark-submit \
	--class "org.fyrz.textclassifier.NewsGroupCrossValidation" \
	--master local[4] \
	build/libs/spark-java-text-classifier-0.1-all.jar

