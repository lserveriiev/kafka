package io.lenur.kafka.joinstreams;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.lenur.kafka.joinstreams.avro.DummyEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.JoinWindows;

import java.util.concurrent.TimeUnit;

@RestController
public class KafkaConsumerController {

    @Autowired
    @Qualifier("consumerStringProperties")
    private Properties consumerStringProperties;

    @Autowired
    @Qualifier("consumerAvroProperties")
    private Properties consumerAvroProperties;

    @Autowired
    private Config config;

    private KafkaStreams streamsInnerJoinString;
    private KafkaStreams streamsMultipleInnerJoinString;
    private KafkaStreams streamsInnerJoinSchema;
    private KafkaStreams streamsMultipleInnerJoinSchema;

    @PostMapping("/consumer/string/inner-join")
    public void streamStringInnerJoin() {
        stop();
        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> firstStream = builder.stream(Constant.FIRST_STRING_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> secondStream = builder.stream(Constant.SECOND_STRING_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> joined = firstStream.join(secondStream,
                (firstValue, secondValue) -> "first=" + firstValue + ", second=" + secondValue, /* ValueJoiner */
                JoinWindows.of(TimeUnit.HOURS.toMillis(1)),
                StreamJoined.with(
                        Serdes.String(), /* key */
                        Serdes.String(),   /* left value */
                        Serdes.String() /* right value */
                )
        );

        joined.to(Constant.INNER_STRING_TOPIC);

        final Topology topology = builder.build();
        consumerStringProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "spike-stream-inner-join-string");
        streamsInnerJoinString = new KafkaStreams(topology, consumerStringProperties);
        streamsInnerJoinString.start();
    }

    @PostMapping("/consumer/string/multiple-stream-inner-join")
    public void multipleStreamStringInnerJoin() {
        stop();

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> firstStream = builder.stream(Constant.FIRST_STRING_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> secondStream = builder.stream(Constant.SECOND_STRING_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> thirdStream = builder.stream(Constant.THIRD_STRING_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> joined = firstStream.join(secondStream,
                (firstValue, secondValue) -> "first=" + firstValue + ", second=" + secondValue, /* ValueJoiner */
                JoinWindows.of(TimeUnit.HOURS.toMillis(1)),
                StreamJoined.with(
                        Serdes.String(), /* key */
                        Serdes.String(),   /* left value */
                        Serdes.String() /* right value */
                )
        ).join(thirdStream,
                (merged, thirdValue) -> "merged=" + merged + ", thirdValue=" + thirdValue, /* ValueJoiner */
                JoinWindows.of(TimeUnit.HOURS.toMillis(1)),
                StreamJoined.with(
                        Serdes.String(), /* key */
                        Serdes.String(),   /* left value */
                        Serdes.String() /* right value */
                )
        );

        joined.to(Constant.INNER_MULTIPLE_STRING_TOPIC);

        final Topology topology = builder.build();
        consumerStringProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "spike-stream-multiple-inner-join-string");
        streamsMultipleInnerJoinString = new KafkaStreams(topology, consumerStringProperties);
        streamsMultipleInnerJoinString.start();
    }

    @PostMapping("/consumer/schema/inner-join")
    public void startStreamSchemaInnerJoin() {
        stop();

        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                config.getKafkaSchemaRegistryUrl());

        final Serde<DummyEvent> valueSpecificAvroSerde = new SpecificAvroSerde<>();
        valueSpecificAvroSerde.configure(serdeConfig, false);

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, DummyEvent> firstStream = builder.stream(Constant.FIRST_SCHEMA_TOPIC,
                Consumed.with(Serdes.String(), valueSpecificAvroSerde));
        KStream<String, DummyEvent> secondStream = builder.stream(Constant.SECOND_SCHEMA_TOPIC,
                Consumed.with(Serdes.String(), valueSpecificAvroSerde));

        var joined = firstStream.join(secondStream,
                (firstValue, secondValue) -> new DummyEventValueJoiner().apply(firstValue, secondValue),
                JoinWindows.of(TimeUnit.HOURS.toMillis(1)),
                StreamJoined.with(
                        Serdes.String(),
                        valueSpecificAvroSerde,
                        valueSpecificAvroSerde)
        );

        joined.to(Constant.INNER_SCHEMA_TOPIC);

        final Topology topology = builder.build();
        consumerAvroProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "spike-stream-inner-join-schema-application-id");
        streamsInnerJoinSchema = new KafkaStreams(topology, consumerAvroProperties);
        streamsInnerJoinSchema.start();
    }

    @PostMapping("/consumer/schema/multiple-inner-join")
    public void schemaMultipleInnerJoin() {
        stop();

        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                config.getKafkaSchemaRegistryUrl());

        final Serde<DummyEvent> valueSpecificAvroSerde = new SpecificAvroSerde<>();
        valueSpecificAvroSerde.configure(serdeConfig, false);

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, DummyEvent> firstStream = builder.stream(Constant.FIRST_SCHEMA_TOPIC,
                Consumed.with(Serdes.String(), valueSpecificAvroSerde));
        KStream<String, DummyEvent> secondStream = builder.stream(Constant.SECOND_SCHEMA_TOPIC,
                Consumed.with(Serdes.String(), valueSpecificAvroSerde));
        KStream<String, DummyEvent> thirdStream = builder.stream(Constant.THIRD_SCHEMA_TOPIC,
                Consumed.with(Serdes.String(), valueSpecificAvroSerde));

        var joined = firstStream.join(secondStream,
                (firstValue, secondValue) -> firstValue,
                JoinWindows.of(TimeUnit.HOURS.toMillis(1)),
                StreamJoined.with(
                        Serdes.String(),
                        valueSpecificAvroSerde,
                        valueSpecificAvroSerde)
        ).join(thirdStream,
                (firstValue, secondValue) -> firstValue,
                JoinWindows.of(TimeUnit.HOURS.toMillis(1)),
                StreamJoined.with(
                        Serdes.String(),
                        valueSpecificAvroSerde,
                        valueSpecificAvroSerde)
        );

        joined.to(Constant.INNER_MULTIPLE_SCHEMA_TOPIC);

        final Topology topology = builder.build();
        consumerAvroProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "spike-stream-multiple-inner-join-schema-application-id");
        streamsMultipleInnerJoinSchema = new KafkaStreams(topology, consumerAvroProperties);
        streamsMultipleInnerJoinSchema.start();
    }

    private void stop() {
        if (streamsInnerJoinString != null) {
            streamsInnerJoinString.close();
        }

        if (streamsMultipleInnerJoinString != null) {
            streamsMultipleInnerJoinString.close();
        }

        if (streamsInnerJoinSchema != null) {
            streamsInnerJoinSchema.close();
        }

        if (streamsMultipleInnerJoinSchema != null) {
            streamsMultipleInnerJoinSchema.close();
        }
    }
}