FROM openjdk:8-jre-alpine

RUN apk update && apk add libc6-compat rocksdb --update-cache --repository http://nl.alpinelinux.org/alpine/edge/testing
RUN ln -s /lib/libc.musl-x86_64.so.1 /usr/lib/ld-linux-x86-64.so.2

ARG JAR_FILE
ADD ${JAR_FILE} /svc.jar
ENTRYPOINT /usr/bin/java -jar /svc.jar
