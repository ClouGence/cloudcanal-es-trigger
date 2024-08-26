#! /bin/bash

SUB_PROJECT=${1}

if [ "${SUB_PROJECT}" == "" ] ; then
  echo "Need pass sub project name."
  exit 1
elif [ "${SUB_PROJECT}" == "es-trigger-es7" ] ; then
  echo "For es-trigger-es7 , need Java version >= 1.8"
elif [ "${SUB_PROJECT}" == "es-trigger-es8" ]; then
  echo "For es-trigger-es8 , need Java version >= 17"
else
  echo "Not supported sub project name:${SUB_PROJECT}"
  exit 1
fi

echo "Current JAVA_HOME is:${JAVA_HOME}"

./gradlew -x test -p ${SUB_PROJECT} clean build
./gradlew -x test -p ${SUB_PROJECT} prepare
./gradlew -x test -p ${SUB_PROJECT} dist