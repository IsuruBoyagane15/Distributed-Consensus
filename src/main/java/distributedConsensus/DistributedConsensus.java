package distributedConsensus;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;

/**
 * API which gives Kafka messaging and Javascript record evaluation services to be used to
 * achieve consensus
 */
public class DistributedConsensus{
    private KafkaConsumer<String, String> kafkaConsumer;
    private KafkaProducer<String, String> kafkaProducer;
    private org.graalvm.polyglot.Context jsContext;
    private ConsensusApplication distributedNode;

    /**
     * Constructor
     *
     * @param distributedNode ConsensusApplication which uses DistributedConsensus API
     */
    public DistributedConsensus(ConsensusApplication distributedNode){
        this.jsContext = Context.create("js");
        this.distributedNode  = distributedNode;
        this.kafkaConsumer = ConsumerGenerator.generateConsumer(distributedNode.getKafkaServerAddress(),
                distributedNode.getKafkaTopic(), distributedNode.getNodeId());
        this.kafkaProducer = ProducerGenerator.generateProducer(distributedNode.getKafkaServerAddress());
    }

    /**
     * Poll Kafka and return a collection of ConsumerRecords
     *
     * @return collection of ConsumerRecords
     */
    public ConsumerRecords<String, String> getMessages(){
        return  this.kafkaConsumer.poll(10);
    }

    /**
     *Close KafkaConsumer connection
     */
    public void closeConsumer(){
        this.kafkaConsumer.close();
    }

    /**
     * Write a Javascript command as a String to Kafka
     * @param command command to write
     */
    public void writeACommand(String command) {
        kafkaProducer.send(new ProducerRecord<String, String>(distributedNode.getKafkaTopic(),
                command));
    }

    /**
     * Concatenate runtimeJsCode and command and set the result to runtimeJsCode
     * Evaluate and return Value of (runtimeJsCode + evaluationJsCode)
     *
     * @param command new Javascript record read from Kafka
     * @return result of evaluation
     */
    public Value evaluateJsCode(String command){
        distributedNode.setRuntimeJsCode(distributedNode.getRuntimeJsCode() + command);
        return jsContext.eval("js",distributedNode.getRuntimeJsCode()+
                distributedNode.getEvaluationJsCode());
    }
}
