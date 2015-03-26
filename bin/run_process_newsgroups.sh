#!/bin/bash

for d in */ ; do
  for f in $d*; do

    # check if f is a file
    if [[ -f $f ]]; then

      content=`tr -d "\n\r" < $f`
      echo "$d ||| $content" >> out
    fi

  done
done

