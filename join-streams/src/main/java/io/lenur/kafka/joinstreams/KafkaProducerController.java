package io.lenur.kafka.joinstreams;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.lenur.kafka.joinstreams.avro.DummyEvent;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;

@RestController
@AllArgsConstructor
@Log4j2
public class KafkaProducerController {

    @Autowired
    @Qualifier("producerStringProperties")
    private final Properties producerStringProperties;

    @Autowired
    @Qualifier("producerAvroProperties")
    private final Properties producerAvroProperties;

    @PostMapping("/producer/string/first/send/{id}")
    public void sendFirstString(@PathVariable int id) {
        var key = "correlationId" + id;
        try (Producer<String, String> producer = new KafkaProducer<>(producerStringProperties)) {
            producer.send(new ProducerRecord<String, String>(Constant.FIRST_STRING_TOPIC, key, key));
        }
    }

    @PostMapping("/producer/string/second/send/{id}")
    public void sendSecondString(@PathVariable int id) {
        var key = "correlationId" + id;
        try (Producer<String, String> producer = new KafkaProducer<>(producerStringProperties)) {
            producer.send(new ProducerRecord<String, String>(Constant.SECOND_STRING_TOPIC, key, key));
        }
    }

    @PostMapping("/producer/string/third/send/{id}")
    public void sendThirdString(@PathVariable int id) {
        var key = "correlationId" + id;
        try (Producer<String, String> producer = new KafkaProducer<>(producerStringProperties)) {
            producer.send(new ProducerRecord<String, String>(Constant.THIRD_STRING_TOPIC, key, key));
        }
    }

    @PostMapping("/producer/schema/first/send/{id}")
    public void sendFirstSchema(@PathVariable int id) {
        var key = "correlationId" + id;
        try (KafkaProducer<String, DummyEvent> producer = new KafkaProducer<>(producerAvroProperties)) {
            producer.send(new ProducerRecord<>(Constant.FIRST_SCHEMA_TOPIC, key, buildDummyEvent(id)));
        }
    }

    @PostMapping("/producer/schema/second/send/{id}")
    public void sendSecondSchema(@PathVariable int id) {
        var key = "correlationId" + id;
        try (KafkaProducer<String, DummyEvent> producer = new KafkaProducer<>(producerAvroProperties)) {
            producer.send(new ProducerRecord<>(Constant.SECOND_SCHEMA_TOPIC, key, buildDummyEvent(id)));
        }
    }

    private DummyEvent buildDummyEvent(int id) {
        return DummyEvent
                .newBuilder()
                .setCorrelationId("correlationId" + id)
                .setDocumentId("documentId" + id)
                .setJobType("jobType" + id)
                .build();
    }
}