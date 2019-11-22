package distributedConsensus;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;

public class DistributedConsensus{
    private KafkaConsumer<String, String> kafkaConsumer;
    private KafkaProducer<String, String> kafkaProducer;
    private org.graalvm.polyglot.Context jsContext;
    private ConsensusApplication distributedNode;

    public DistributedConsensus(ConsensusApplication distributedNode){
        this.jsContext = Context.create("js");
        this.distributedNode  = distributedNode;
        this.kafkaConsumer = ConsumerGenerator.generateConsumer(distributedNode.getKafkaServerAddress(),
                distributedNode.getKafkaTopic(), distributedNode.getNodeId());
        this.kafkaProducer = ProducerGenerator.generateProducer(distributedNode.getKafkaServerAddress());
    }

    public ConsumerRecords<String, String> getMessages(){
        return  this.kafkaConsumer.poll(10);
    }

    public void closeConsumer(){
        this.kafkaConsumer.close();
    }

    public void writeACommand(String command) {
        kafkaProducer.send(new ProducerRecord<String, String>(distributedNode.getKafkaTopic(), command));
    }

    public Value evaluateJsCode(String command){
        distributedNode.setRuntimeJsCode(distributedNode.getRuntimeJsCode() + command);
        return jsContext.eval("js",distributedNode.getRuntimeJsCode()+
                distributedNode.getEvaluationJsCode());
    }

    public String generateUniqueKey(){
        String ALPHA_NUMERIC_STRING = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder builder = new StringBuilder();
        int rawStringLength = 16;
        while (rawStringLength-- != 0) {
            int character = (int)(Math.random()*ALPHA_NUMERIC_STRING.length());
            builder.append(ALPHA_NUMERIC_STRING.charAt(character));
        }
        return builder.toString();
    }

}
