# Development and execution container
FROM eclipse-temurin:21-jdk

ARG SCALA_VERSION=2.13.16
ENV SCALA_VERSION=${SCALA_VERSION}
ARG SPARK_VERSION=4.0.2
ENV SPARK_VERSION=${SPARK_VERSION}
ARG SBT_VERSION=1.10.1

ENV DEBIAN_FRONTEND=noninteractive
ENV JAVA_HOME=/opt/java/openjdk
ENV SCALA_HOME=/opt/scala
ENV SPARK_HOME=/opt/spark
ENV SBT_HOME=/opt/sbt
ENV PATH=$SCALA_HOME/bin:$SPARK_HOME/bin:$SBT_HOME/bin:$PATH

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash curl wget tar gzip unzip python3 procps ca-certificates findutils coreutils \
    && rm -rf /var/lib/apt/lists/*

RUN wget -q https://www.scala-lang.org/files/archive/scala-${SCALA_VERSION}.tgz \
    && tar -xzf scala-${SCALA_VERSION}.tgz -C /opt \
    && ln -s /opt/scala-${SCALA_VERSION} ${SCALA_HOME} \
    && rm scala-${SCALA_VERSION}.tgz

RUN wget -q https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz \
    && tar -xzf sbt-${SBT_VERSION}.tgz -C /opt \
    && ln -s /opt/sbt ${SBT_HOME} || true \
    && rm sbt-${SBT_VERSION}.tgz

RUN wget -q https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt \
    && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} \
    && rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

WORKDIR /workspace
COPY . /workspace

RUN sbt clean package

CMD ["/workspace/run.sh"]
