package distributedConsensus;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

/**
 * Kafka producer generator
 * Generate Kafka producer to be used by DistributedConsensus to write ConsensusApplications messages
 * into kafka
 */
public class ProducerGenerator {

    /**
     * generate and return a KafkaProducer which can write to Kafka at kafkaServerAddress
     *
     * @param kafkaServerAddress URL of Kafka server
     * @return KafkaProducer
     */
    public static KafkaProducer<String, String> generateProducer(String kafkaServerAddress) {

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaServerAddress);
        String serializer = "org.apache.kafka.common.serialization.StringSerializer";
        props.put("key.serializer", serializer);
        props.put("value.serializer", serializer);

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);
        return kafkaProducer;
    }
}
