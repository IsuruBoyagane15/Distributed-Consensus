package distributedConsensus;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerGenerator {

    public static KafkaConsumer<String, String> generateConsumer(String kafkaServer, String topic, String consumerGroupId) {

        Properties props = new Properties();

        props.put("bootstrap.servers", kafkaServer);
        props.put("group.id",consumerGroupId); //specifies consumer group - all the clients which want to come to consensus should be a member of same group
        props.put("enable.auto.commit","true"); //let consumer to commit most recently read offset to kafka - default behaviour
        props.put("auto.commit.interval.ms","1000");
        String deserializer = "org.apache.kafka.common.serialization.StringDeserializer";
        props.put("key.deserializer",deserializer);
        props.put("value.deserializer",deserializer);
        props.put("auto.offset.reset", "earliest"); //set consumer to read the topic from the beginning

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }
}
