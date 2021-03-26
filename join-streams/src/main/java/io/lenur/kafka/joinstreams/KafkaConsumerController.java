package io.lenur.kafka.joinstreams;

import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

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

    private KafkaStreams streamsInnerJoinString;

    @PostMapping("/consumer/string/inner-join")
    public void streamStringInnerJoin() {
        stop();
        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> firstStream = builder.stream(Constant.FIRST_STRING_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> rightStream = builder.stream(Constant.SECOND_STRING_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> joined = firstStream.join(rightStream,
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

    private void stop () {
        if (streamsInnerJoinString != null) {
            streamsInnerJoinString.close();
        }
    }
}