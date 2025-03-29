package com.learnkafkastreams.serdes;

import com.learnkafkastreams.domain.Greeting;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdesFactory {

    public static Serde<Greeting> greetingSerdes() {
        return new GreetingSerdes();
    }

    public static Serde<Greeting> greetingSerdesFromGenericSerializer() {
        JsonSerializer<Greeting> greetingSerializer = new JsonSerializer<>();
        JsonDeserializer<Greeting> greetingDeserializer = new JsonDeserializer<>(Greeting.class);
        return Serdes.serdeFrom(greetingSerializer, greetingDeserializer);
    }
}
