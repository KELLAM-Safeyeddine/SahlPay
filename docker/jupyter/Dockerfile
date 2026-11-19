FROM spark:3.5.3-scala2.12-java17-python3-ubuntu
USER root
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYSPARK_PYTHON=python3 PYTHONPATH=/workspace/src:/opt/graphframes
RUN apt-get update && apt-get install -y --no-install-recommends curl unzip ca-certificates && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
RUN curl -fL --retry 3 https://repos.spark-packages.org/graphframes/graphframes/0.8.3-spark3.5-s_2.12/graphframes-0.8.3-spark3.5-s_2.12.jar -o /tmp/graphframes.jar \
    && mkdir /opt/graphframes && unzip -q /tmp/graphframes.jar 'graphframes/*' -d /opt/graphframes && rm /tmp/graphframes.jar \
    && curl -fL --retry 3 https://repo.maven.apache.org/maven2/org/scala-sbt/sbt-launch/1.10.2/sbt-launch-1.10.2.jar -o /opt/sbt-launch.jar
COPY scripts/sbt.sh /usr/local/bin/sbt
RUN chmod +x /usr/local/bin/sbt
WORKDIR /workspace
ENTRYPOINT []
