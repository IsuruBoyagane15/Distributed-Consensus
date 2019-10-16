package distributed_consensus_se;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class DistributedConsensus {
    private KafkaConsumer<String, String> kafkaConsumer;
    private KafkaProducer<String, String> kafkaProducer;
    private org.graalvm.polyglot.Context jsContext;
    private ConsensusApplication distributedNode;

    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedConsensus.class);
    private static DistributedConsensus instance = null;
    private boolean terminate;

    private DistributedConsensus(ConsensusApplication distributedNode){
        this.jsContext = Context.create("js");
        this.distributedNode  = distributedNode;
        this.kafkaConsumer = ConsumerGenerator.generateConsumer(distributedNode.getKafkaServerAddress(),
                distributedNode.getKafkaTopic(), distributedNode.getNodeId());
        this.kafkaProducer = ProducerGenerator.generateProducer(distributedNode.getKafkaServerAddress());
        this.terminate = false;
    }

    public static DistributedConsensus getDistributeConsensus(ConsensusApplication distributedNode){
        if (instance == null){
            synchronized (DistributedConsensus.class){
                if (instance == null){
                    instance = new DistributedConsensus(distributedNode);
                }
            }
        }
        return instance;
    }

    public void start(){

        final String initialJsCode = this.distributedNode.getRuntimeJsCode();

        writeACommand("IN,"+ distributedNode.getNodeId());

        Runnable consuming = new Runnable() {
            public void run() {
//                boolean consensusAchieved = false;
                boolean correctRoundIdentified = false;
                ArrayList<String> participantIds = new ArrayList<String>();
                try {
                    while (!terminate) {
                        ConsumerRecords<String, String> records = kafkaConsumer.poll(10);
                        for (ConsumerRecord<String, String> record : records) {
                            if (record.value().startsWith("IN,")) {
                                String[] inCommand = record.value().split(",");
                                participantIds.add(inCommand[1]);
                                if (inCommand[1].equals(distributedNode.getNodeId())) {
                                    correctRoundIdentified = true;
                                }
                            }
                            else if (record.value().startsWith("RESET,")){
                                String[] resetCommands = record.value().split(",");
                                participantIds.remove(resetCommands[1]);
                                if (participantIds.size() == 0){
                                    distributedNode.setRuntimeJsCode(initialJsCode);
                                }
                            }
                            else if(record.value().startsWith("ALIVE,")){
                                distributedNode.commitAgreedValue(null);
                            }
                            else{
                                Value result = evaluateJsCode(record.value());
                                boolean consensusAchieved = distributedNode.onReceiving(result);
                                if (consensusAchieved){
                                    if (correctRoundIdentified){
                                        distributedNode.commitAgreedValue(result);
                                        writeACommand("RESET,"+ distributedNode.getNodeId());
                                    }
//                                    else{
//                                        consensusAchieved = false;
//                                    }
                                }
                            }
                        }
                    }
                } catch(Exception exception) {
                    LOGGER.error("Exception occurred :", exception);
                }finally {
                    kafkaConsumer.close();
                }
            }
        };
        new Thread(consuming).start();
    }

    public void writeACommand(String command) {
        kafkaProducer.send(new ProducerRecord<String, String>(distributedNode.getKafkaTopic(), command));
    }

    public Value evaluateJsCode(String command){
        distributedNode.setRuntimeJsCode(distributedNode.getRuntimeJsCode() + command);
        return jsContext.eval("js",distributedNode.getRuntimeJsCode()+
                distributedNode.getEvaluationJsCode());
    }

    public void setTerminate(boolean terminate) {
        this.terminate = terminate;
    }

    public void finishRound(String nodeId){
        writeACommand("RESET," + nodeId);
    };

}
