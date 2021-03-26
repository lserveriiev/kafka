package io.lenur.kafka.joinstreams;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
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
}