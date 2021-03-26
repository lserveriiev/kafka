package io.lenur.kafka.joinstreams;

import io.lenur.kafka.joinstreams.avro.DummyEvent;
import io.lenur.kafka.joinstreams.avro.DummyEventJoiner;
import org.apache.kafka.streams.kstream.ValueJoiner;

public class DummyEventValueJoiner implements ValueJoiner<DummyEvent, DummyEvent, DummyEventJoiner> {

    @Override
    public DummyEventJoiner apply(DummyEvent event1, DummyEvent event2) {
        return DummyEventJoiner.newBuilder()
                .setCorrelationIdFirst(event1.getCorrelationId())
                .setDocumentIdFirst(event1.getDocumentId())
                .setJobTypeFirst(event1.getJobType())
                .setCorrelationIdSecond(event2.getCorrelationId())
                .setDocumentIdSecond(event2.getDocumentId())
                .setJobTypeSecond(event2.getJobType())
                .build();
    }
}
