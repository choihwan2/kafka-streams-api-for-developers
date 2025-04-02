package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Greeting;
import com.learnkafkastreams.serdes.SerdesFactory;
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

//        var mergedStream = getStringGreetingKStream(streamsBuilder);

        var mergedStream = getCustomGreetingKStream(streamsBuilder);
        mergedStream.print(Printed.<String, Greeting>toSysOut().withLabel("mergedStream"));

//        var modifiedStream = exploreOperators(mergedStream);

        KStream<String, Greeting> modifiedStream = exploreErrors(mergedStream);

        modifiedStream.print(Printed.<String, Greeting>toSysOut().withLabel("modifiedStream"));
        modifiedStream.to(GREETINGS_UPPERCASE
                , Produced.with(Serdes.String(), SerdesFactory.greetingSerdesFromGenericSerializer())
        );

        return streamsBuilder.build();
    }

    private static KStream<String, Greeting> exploreErrors(KStream<String, Greeting> mergedStream) {
        return mergedStream.mapValues((readOnlyKey, value) -> {
                    if (value.message().equals("Transient Error")) {
                        try {
                            throw new IllegalStateException(value.message());
                        } catch (Exception e) {
                            log.error("Exception in exploreErros : {}", e.getMessage(), e);
                            return null;
                        }
                    }
                    return new Greeting(value.message().toUpperCase(), value.timeStamp());
                }
        ).filter((key, value) -> key!= null && value != null)
                ;
    }

    private static KStream<String, Greeting> exploreOperators(KStream<String, Greeting> mergedStream) {
        var modifiedStream = mergedStream
                .mapValues(value -> new Greeting(value.message().toUpperCase(), value.timeStamp()))
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
//        modifiedStream.print(Printed.<String, String>toSysOut().withLabel("modifiedStream"));
        return modifiedStream;
    }

    private static KStream<String, String> exploreOperators(StreamsBuilder streamsBuilder) {
        KStream<String, String> greetingStream = streamsBuilder.stream(GREETINGS);
        KStream<String, String> greetingsSpanishStream = streamsBuilder.stream(GREETINGS_SPANISH
        );

        var mergedStream = greetingStream.merge(greetingsSpanishStream);
        return mergedStream;
    }

    private static KStream<String, Greeting> getCustomGreetingKStream(StreamsBuilder streamsBuilder) {
        var greetingStream = streamsBuilder.stream(GREETINGS,
                Consumed.with(Serdes.String(), SerdesFactory.greetingSerdesFromGenericSerializer()));
        var greetingsSpanishStream = streamsBuilder.stream(GREETINGS_SPANISH,
                Consumed.with(Serdes.String(), SerdesFactory.greetingSerdesFromGenericSerializer())
        );

        var mergedStream = greetingStream.merge(greetingsSpanishStream);
        return mergedStream;
    }
}
