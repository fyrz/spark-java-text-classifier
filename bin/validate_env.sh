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
