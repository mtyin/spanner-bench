#!/bin/bash

export GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS=TRUE
mvn clean compile exec:java  -Dexec.mainClass="org.example.ThroughputRunner" -Dexec.jvmArgs="-Xms4g -Xmx16g -XX:+UseG1GC"  -Dexec.args="-project google.com:cloud-spanner-demo -instance mtyin-wmt-demo -database demo -experiment $1 -numKeys 1000000 -numOperations 1000000 -concurrency 5"
