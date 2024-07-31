#! /bin/bash

./gradlew -x test clean build
./gradlew -x test prepare
./gradlew -x test dist
