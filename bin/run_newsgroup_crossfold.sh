#!/bin/bash

# SPARK_HOME validation
function validate_spark_home {
  if [ -z ${SPARK_HOME} ]
  then
    echo "SPARK_HOME must be set in environment."
	exit 1
  else
    if [ ! -e ${SPARK_HOME} ]
    then
      echo "SPARK_HOME must be set properly in environment."
	  exit 1
    fi
  fi
}

validate_spark_home
# Use spark-submit to run your application
$SPARK_HOME/bin/spark-submit \
	--class "org.fyrz.textclassifier.NewsGroupCrossValidation" \
	--master local[4] \
	build/libs/spark-newsgroup-classifier-0.1-all.jar

