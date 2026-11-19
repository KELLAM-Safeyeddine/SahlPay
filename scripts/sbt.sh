#!/usr/bin/env sh
set -eu
exec java -Xms128m -Xmx768m -Dsbt.supershell=false -Dsbt.override.build.repos=true \
  -Dsbt.repository.config=/workspace/conf/sbt-repositories -jar /opt/sbt-launch.jar "$@"
