# Java implementation of joining of Kafka streams 

### Run application
### Build application image
```bash
gradle clean build;

./application-image-build.sh
```

### Run application
```bash
docker-compose up -d
```

### Join string message based topics

#### Join two streams

##### Create the messages
```bash
curl -X POST http://127.0.0.1:9090/producer/string/first/send/1
curl -X POST http://127.0.0.1:9090/producer/string/second/send/1
```

###### Obtain the messages
```bash
./kafka-console-consumer --bootstrap-server kafka-join-stream:9092 --topic spike-first-stream-topic-string --from-beginning
./kafka-console-consumer --bootstrap-server kafka-join-stream:9092 --topic spike-second-stream-topic-string --from-beginning
```

###### Join the streams
```bash
curl -X POST http://127.0.0.1:9090/consumer/string/inner-join
```

###### Obtain the messages
```bash
./kafka-console-consumer --bootstrap-server kafka-join-stream:9092 --topic spike-inner-stream-topic-string --from-beginning
```

#### Join several streams

##### Create the messages
```bash
curl -X POST http://127.0.0.1:9090/producer/string/first/send/1
curl -X POST http://127.0.0.1:9090/producer/string/second/send/1
curl -X POST http://127.0.0.1:9090/producer/string/third/send/1
```

###### Obtain the messages
```bash
./kafka-console-consumer --bootstrap-server kafka-join-stream:9092 --topic spike-first-stream-topic-string --from-beginning
./kafka-console-consumer --bootstrap-server kafka-join-stream:9092 --topic spike-second-stream-topic-string --from-beginning
./kafka-console-consumer --bootstrap-server kafka-join-stream:9092 --topic spike-third-stream-topic-string --from-beginning
```

###### Join the streams
```bash
curl -X POST http://127.0.0.1:9090/consumer/string/multiple-stream-inner-join
```

###### Obtain the messages
```bash
./kafka-console-consumer --bootstrap-server kafka-join-stream:9092 --topic spike-multiple_inner-stream-topic-string --from-beginning
```
