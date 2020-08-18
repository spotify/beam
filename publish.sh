#!/usr/bin/env bash

set -eu

[ -f $HOME/.m2/settings.xml ] || { echo "Missing ~/.m2/settings.xml"; exit 1; }

./gradlew \
    -PdistMgmtServerId=snapshots-upload \
    -PdistMgmtSnapshotsUrl=https://artifactory.spotify.net/artifactory/libs-snapshot-local \
    -Ppublishing \
    :model:fn-execution:publish \
    :model:job-management:publish \
    :model:pipeline:publish \
    :runners:core-construction-java:publish \
    :runners:core-java:publish \
    :runners:direct-java:publish \
    :runners:google-cloud-dataflow-java:publish \
    :runners:java-fn-execution:publish \
    :runners:java-job-service:publish \
    :runners:flink:1.11:publish \
    :runners:flink:1.11:job-server:publish \
    :runners:spark:publish \
    :runners:spark:job-server:publish \
    :sdks:java:core:publish \
    :sdks:java:expansion-service:publish \
    :sdks:java:extensions:google-cloud-platform-core:publish \
    :sdks:java:extensions:join-library:publish \
    :sdks:java:extensions:protobuf:publish \
    :sdks:java:extensions:sorter:publish \
    :sdks:java:extensions:sql:publish \
    :sdks:java:fn-execution:publish \
    :sdks:java:io:google-cloud-platform:publish \
    :sdks:java:io:hadoop-common:publish \
    :sdks:java:io:hadoop-format:publish \
    :sdks:java:io:jdbc:publish \
    :sdks:java:io:mongodb:publish \
    :vendor:sdks-java-extensions-protobuf:publish
