# Syncheror
![Build Status](https://circleci.com/gh/RichoDemus/syncheror.svg?style=shield&circle-token=1d1ea7f47cbff2b6ba9ad5453313432b57a9a7f8)

A tool to sync data between Google Cloud Storage and Kafka  
a work in progress

## Running
`docker run -it --rm -e GCS_PROJECT=X -e GCS_BUCKET=X -e KAFKA_SERVERS=X:9092 -e KAFKA_TOPIC=X richodemus/syncheror`

## Development
Compile and run tests  
`./gradlew`

See the dependency graph of all the gradle tasks  
`./gradlew taskTree build`
