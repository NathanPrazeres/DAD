#!/bin/bash

mvn clean install
cd server

mvn exec:java -Dexec.args="8080 0" &

for i in {1..4}; do
  sleep 10
  mvn exec:java -Dexec.args="8080 $i" &
done

wait
