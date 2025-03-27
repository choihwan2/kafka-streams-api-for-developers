package com.learnkafkastreams.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

@Slf4j
public class GreetingsTopology {
    public static String GREETINGS = "greetings";

    public static String GREETINGS_UPPERCASE = "greetings-uppercase";

    public static String GREETINGS_SPANISH = "greetings-spanish";

    public static Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String,String> greetingStream = streamsBuilder.stream(GREETINGS);
        KStream<String,String> greetingsSpanishStream = streamsBuilder.stream(GREETINGS_SPANISH
//                , Consumed.with(Serdes.String(), Serdes.String())
        );

        var mergedStream = greetingStream.merge(greetingsSpanishStream);

        mergedStream.print(Printed.<String, String>toSysOut().withLabel("mergedStream"));

        var modifiedStream = mergedStream
                .mapValues(value -> value.toUpperCase())
//                .filter((key, value) -> value.length() > 5)
//                .peek((key, value) -> log.info("after filter key : {} value : {}", key, value))
//                .mapValues((readonlyKey, value) -> value.toUpperCase())
//                .peek((key, value) -> log.info("after mapValues : key : {} value : {}", key, value));
//                .map((key, value) -> KeyValue.pair(key.toUpperCase(), value.toUpperCase()));
//                .flatMap((key, value) -> {
//                    var newValues = Arrays.asList(value.split(""));
//                    return newValues.stream()
//                            .map(val -> KeyValue.pair(key, val))
//                            .collect(Collectors.toList());
//                });
        ;
        modifiedStream.print(Printed.<String, String>toSysOut().withLabel("modifiedStream"));
        modifiedStream.to(GREETINGS_UPPERCASE
//                , Produced.with(Serdes.String(), Serdes.String())
        );

        return streamsBuilder.build();
    }
}
