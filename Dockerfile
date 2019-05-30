FROM openjdk:8u151-jre-alpine

ENV KRONOS_VERSION 2.2.3
ENV KRONOS_HOME /home/kronos-${KRONOS_VERSION}
ENV MODE all

RUN mkdir -p ${KRONOS_HOME}

RUN apk --update add curl tar bash

COPY app/target/kronos-2.2.3-dist.tar.gz kronos-${KRONOS_VERSION}-dist.tar.gz

RUN tar -xvzf kronos-${KRONOS_VERSION}-dist.tar.gz -C /home \
  && rm kronos-${KRONOS_VERSION}-dist.tar.gz

VOLUME /home/kronos-${KRONOS_VERSION}/conf
VOLUME /home/kronos-${KRONOS_VERSION}/lib/ext

WORKDIR ${KRONOS_HOME}/sbin

CMD ["sh", "-c", "./kronos.sh start ${MODE}"]
