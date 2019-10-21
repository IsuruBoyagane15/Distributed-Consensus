package distributed_consensus_se;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.server.Activation$ActivationSystemImpl_Stub;

import java.util.ArrayList;

public class DistributedConsensus{
    private KafkaConsumer<String, String> kafkaConsumer;
    private KafkaProducer<String, String> kafkaProducer;
    private org.graalvm.polyglot.Context jsContext;
    private ConsensusApplication distributedNode;

    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedConsensus.class);
    private static DistributedConsensus instance = null;
    private boolean terminate;
    enum roundStatuses {
        ONGOING,
        NEW
    }

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

    public void startRound(){
//
//        String rawString = generateUniqueKey();
//        String unique_round_key = DigestUtils.sha256Hex(rawString);
//        final String checkRecord = "CHECK,"+ unique_round_key;
//        writeACommand(checkRecord);
//
//        final String initialJsCode = this.distributedNode.getRuntimeJsCode();
//
//        Runnable consuming = new Runnable() {
//            public void run() {
//                boolean correctRoundIdentified = false;
//                String latestRoundsJsCode = "";
//
//                int latestRoundNumber = 0;
//                String nextRoundCode = "";
//                int nextRoundNumber = 0;
//                roundStatuses nextRoundStatus;
//
//                try {
//                    while (!terminate) {
//                        ConsumerRecords<String, String> records = kafkaConsumer.poll(10);
//                        for (ConsumerRecord<String, String> record : records) {
//                            if (!correctRoundIdentified){
//                                if (record.value().equals(checkRecord)) {
//                                    //take decision on round and state
//                                    System.out.println("UNIQUE HASH IS FOUND :: IDENTIFYING ROUND TO PARTICIPATE...");
//                                    if (latestRoundsJsCode == ""){
//                                        System.out.println("TOPIC IS EMPTY :: STARTING NEW ROUND WITH ROUND_NUMBER = 0 ...");
//                                        nextRoundStatus = roundStatuses.NEW;
//                                        nextRoundCode = "";
//                                        nextRoundNumber = latestRoundNumber;
//                                    }
//                                    else{
//                                        Value latestRoundResult = evaluateJsCode(latestRoundsJsCode);
//                                        boolean consensusAchieved = distributedNode.onReceiving(latestRoundResult);
//                                        if (consensusAchieved){
//                                            System.out.println("PREVIOUS ROUND IS FINISHED :: STARTING NEW ROUND WITH ROUND_NUMBER = 0 ...");
//                                            nextRoundNumber = latestRoundNumber + 1;
//                                            nextRoundStatus = roundStatuses.NEW;
//                                            nextRoundCode = "";
//                                        }
//                                        else{
//                                            System.out.println("ONGOING ROUND :: PASS THIS ROUND'S NUMBER AND CODES TO CONSENSUS APPLICATION...");
//                                            nextRoundNumber = latestRoundNumber;
//                                            nextRoundStatus = roundStatuses.ONGOING;
//                                            nextRoundCode = latestRoundsJsCode;
//                                        }
//                                    }
//                                    System.out.println(nextRoundStatus+ "-"+ nextRoundNumber + "-" + nextRoundCode);
//                                    distributedNode.setRuntimeJsCode(initialJsCode);
//                                    correctRoundIdentified = distributedNode.participate(nextRoundStatus,nextRoundNumber,nextRoundCode);
//                                    System.out.println("ROUND IDENTIFICATION IS FINISHED...");
//                                }
//                                else if (!record.value().startsWith("CHECK,")){
//                                    //collect most recent rounds codes
//                                    String[] recordContent = record.value().split(",", 2);
//                                    int recordRoundNumber = Integer.parseInt(recordContent[0]);
//                                    String recordCode = recordContent[1];
//                                    if (recordRoundNumber>latestRoundNumber){
//                                        latestRoundsJsCode = recordCode;
//                                        latestRoundNumber = recordRoundNumber;
//                                    }
//                                    else if(recordRoundNumber==latestRoundNumber){
//                                        latestRoundsJsCode += recordCode;
//                                    }
//                                }
//
//                            }
//                            else if (!record.value().startsWith("CHECK,")){
//                                System.out.println("in the correct round");
//                                Value result = evaluateJsCode(record.value().split(",",2)[1]);
//                                boolean consensusAchieved = distributedNode.onReceiving(result);
//                                if (consensusAchieved) {
//                                    distributedNode.commitAgreedValue(result);
//                                }
//                            }
//                        }
//                    }
//                } catch(Exception exception) {
//                    LOGGER.error("Exception occurred :", exception);
//                    System.out.println(exception);
//                }finally {
//                    kafkaConsumer.close();
//                }
//            }
//        };
//        new Thread(consuming).start();
    }

    public void start(){
        final String initialJsCode = this.distributedNode.getRuntimeJsCode();

        Runnable consuming = new Runnable() {
            public void run() {
                try {
                    while (!terminate) {
                        ConsumerRecords<String, String> records = kafkaConsumer.poll(10);
                        for (ConsumerRecord<String, String> record : records) {
                                Value result = evaluateJsCode(record.value());
                                boolean consensusAchieved = distributedNode.onReceiving(result);
                                if (consensusAchieved) {
                                    distributedNode.commitAgreedValue(result);
                                }
                            }

                        }
                } catch(Exception exception) {
                    LOGGER.error("Exception occurred :", exception);
                    System.out.println(exception);
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
