package com.learnkafkastreams.exception;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Map;

@Slf4j
public class StreamsDeserializationExceptionHandler implements DeserializationExceptionHandler {

    int errorCounter = 0;

    @Override
    public DeserializationHandlerResponse handle(ProcessorContext context, ConsumerRecord<byte[], byte[]> consumerRecord, Exception e) {
        log.error("Exception is : {}, and the Kafka record is : {} ", e.getMessage(), consumerRecord);
        log.info("errorCounter is : {} ", errorCounter);
        if (errorCounter < 2) {
            errorCounter++;
            return DeserializationHandlerResponse.CONTINUE;
        }
        return DeserializationHandlerResponse.FAIL;
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
