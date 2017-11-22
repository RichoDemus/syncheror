FROM openjdk:9-b181-jre-slim
RUN mkdir /syncheror
COPY build/dependencies /syncheror/jars
WORKDIR /syncheror
ENTRYPOINT []
CMD java \
-Dsyncheror.gcs.project=$GCS_PROJECT \
-Dsyncheror.gcs.bucket=$GCS_BUCKET \
-Dsyncheror.kafka.bootstrapServers=$KAFKA_SERVERS \
-Dsyncheror.kafka.topic=$KAFKA_TOPIC \
-Dsyncheror.syncDirection=$SYNC_DIRECTION \
-Dsyncheror.newImplementation=$NEW_IMPLEMENTATION \
-XX:+UseConcMarkSweepGC \
-XX:+CMSParallelRemarkEnabled \
-server \
-classpath "jars/*" \
com.richodemus.syncheror.core.MainKt
