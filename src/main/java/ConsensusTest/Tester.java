import distributedConsensus.ConsumerGenerator;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Tester {
    KafkaConsumer consumer = ConsumerGenerator.generateConsumer("localhost:9092", "election", "tester");

}
