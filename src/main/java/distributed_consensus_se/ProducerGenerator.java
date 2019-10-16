package distributed_consensus_se;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

public class ProducerGenerator {

    public static KafkaProducer<String, String> generateProducer(String kafkaServer) {

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaServer);
        String serializer = "org.apache.kafka.common.serialization.StringSerializer";
        props.put("key.serializer", serializer);
        props.put("value.serializer", serializer);

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);
        return kafkaProducer;
    }
}
