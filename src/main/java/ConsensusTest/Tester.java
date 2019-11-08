package ConsensusTest;

import distributedConsensus.ConsumerGenerator;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.UUID;

public class Tester {
    KafkaConsumer consumer = ConsumerGenerator.generateConsumer("localhost:9092", "election", "tester");

    public static void main(String args[]){
        String nodeId = UUID.randomUUID().toString();

        ProcessBuilder pb = new ProcessBuilder("java", "-jar", "/home/isurub/IdeaProjects/distributedconsensus-se/target/distributed.consensus-1.0-jar-with-dependencies.jar", "localhost:9092", "election", nodeId);
        try {
            Process p = pb.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        pb.directory();
    }


}
