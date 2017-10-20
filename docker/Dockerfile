FROM openjdk:9-b181-jre-slim
RUN mkdir /syncheror
COPY build/dependencies /syncheror/jars
WORKDIR /syncheror
ENTRYPOINT []
CMD java \
-Dchronicler.storage=$STORAGE \
-Dchronicler.gcs.project=$GCS_PROJECT \
-Dchronicler.gcs.bucket=$GCS_BUCKET \
-Dchronicler.saveToDisk=$SAVE_TO_DISK \
-Dchronicler.dataDir=$DATA_DIR \
-XX:+UseConcMarkSweepGC \
-XX:+CMSParallelRemarkEnabled \
-server \
-classpath "jars/*" \
com.richodemus.syncheror.core.MainKt
