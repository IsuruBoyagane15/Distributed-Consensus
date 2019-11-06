package distributedConsensus;

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
        NEW,
        FINISHED
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

    public void start(){
        Runnable consuming = () -> {
            try {
                while (!terminate) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(10);
                    for (ConsumerRecord<String, String> record : records) {
                        Value result = evaluateJsCode(record.value());
                        boolean consensusAchieved = distributedNode.onReceiving(result);
                        if (consensusAchieved) {
                            distributedNode.onConsensus(result);
                        }
                    }
                }
            } catch(Exception exception) {
                LOGGER.error("Exception occurred :", exception);
                System.out.println(exception);
            }finally {
                kafkaConsumer.close();
            }
        };
        new Thread(consuming).start();
    }

    public void startRound(){

        String rawString = generateUniqueKey();
        String unique_round_key = DigestUtils.sha256Hex(rawString);
        final String checkRecord = "CHECK,"+ unique_round_key;
        final int[] ongoingRoundNumber = {0};

        System.out.println("My ID ID " + distributedNode.getNodeId() + " :: " + "CHECK MESSAGE IS " + checkRecord);
        writeACommand(checkRecord);


        Runnable consuming = () -> {
            boolean correctRoundIdentified = false;

            String latestRoundsJsCode = "";
            int latestRoundNumber = 0;

            String nextRoundCode;
            int proposedRoundNumber = 0;
            roundStatuses nextRoundStatus;

            try {
                while (!terminate) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(10);
                    for (ConsumerRecord<String, String> record : records) {
                        if (!correctRoundIdentified){
                            //IDENTIFYING THE ROUND
                            if (record.value().equals(checkRecord)) {
                                //take decision on round and state AFTER CHECHRECORD IS FOUND
//                                System.out.println("UNIQUE HASH IS FOUND :: IDENTIFYING ROUND TO PARTICIPATE...");
                                if (latestRoundsJsCode == ""){
//                                    System.out.println("TOPIC IS EMPTY :: STARTING NEW ROUND WITH ROUND_NUMBER = 0 ...");
                                    nextRoundStatus = roundStatuses.NEW;
                                    nextRoundCode = "";
                                }
                                else{
                                    Value latestRoundResult = evaluateJsCode(latestRoundsJsCode);
                                    boolean consensusAchieved = distributedNode.checkConsensus(latestRoundResult);
                                    if (consensusAchieved){
//                                        System.out.println("PREVIOUS ROUND IS FINISHED :: STARTING NEW ROUND WITH ROUND_NUMBER = 0 ...");
                                        nextRoundStatus = roundStatuses.FINISHED;
                                        nextRoundCode = "";
                                        proposedRoundNumber = latestRoundNumber;
                                    }
                                    else{
//                                        System.out.println("ONGOING ROUND :: PASS THIS ROUND'S NUMBER AND CODES TO CONSENSUS APPLICATION...");
                                        nextRoundStatus = roundStatuses.ONGOING;
                                        nextRoundCode = latestRoundsJsCode;
                                        proposedRoundNumber = latestRoundNumber;
                                    }
                                }
                                System.out.println("\nNEXT ROUND STATUS is " + nextRoundStatus);
                                System.out.println("NEXT ROUND NUMBER is " + proposedRoundNumber);
                                System.out.println("NEXT ROUND JSCODE is " + nextRoundCode + "\n");


                                distributedNode.participate(nextRoundStatus,proposedRoundNumber,nextRoundCode);
//                                System.out.println("ROUND IDENTIFICATION IS FINISHED...");
                                correctRoundIdentified = true;

                            }
                            else if (!record.value().startsWith("CHECK,")){
                                //collect most recent rounds codes

                                String[] recordContent = record.value().split(",", 2);
                                int recordRoundNumber = Integer.parseInt(recordContent[0]);
                                String recordMessage = recordContent[1]; //ALIVE or clean JS
                                if (!recordMessage.startsWith("ALIVE")){
                                    if (recordRoundNumber>latestRoundNumber){
                                        latestRoundsJsCode = recordMessage;
                                        latestRoundNumber = recordRoundNumber;
                                    }
                                    else if(recordRoundNumber==latestRoundNumber){
                                        latestRoundsJsCode += recordMessage;
                                    }
                                }
                            }
                        }
                        else if (!record.value().startsWith("CHECK,")){
                            //EVALUATING RECORDS OF CURRENT ROUND
                            String[] recordContent = record.value().split(",", 2);
                            int recordRoundNumber = Integer.parseInt(recordContent[0]); //Round number written with the record
                            String recordMessage = recordContent[1]; //ALIVE,nodeId or clean JS

                            if (recordRoundNumber > ongoingRoundNumber[0]){
                                distributedNode.cleanRound();
                                ongoingRoundNumber[0] = recordRoundNumber;
                            }
                            if(recordMessage.startsWith("ALIVE")){
                                distributedNode.handleHeartbeat();
                            }
                            else{
                                Value result = evaluateJsCode(recordMessage);
//                                System.out.println("RESULT IS : " + result);
                                boolean consensusAchieved = distributedNode.onReceiving(result);
                                if (consensusAchieved) {
                                    distributedNode.onConsensus(result);
                                }
                            }
                        }
                    }
                }
            } catch(Exception exception) {
                LOGGER.error("Exception occurred :", exception);
                System.out.println(exception);
            }finally {
                kafkaConsumer.close();
            }
        };
        new Thread(consuming).start();
    }

    public void writeACommand(String command) {
        kafkaProducer.send(new ProducerRecord<String, String>(distributedNode.getKafkaTopic(), command));
    }

    public Value evaluateJsCode(String command){
        //concatinate current runtimeJsCode with command
        distributedNode.setRuntimeJsCode(distributedNode.getRuntimeJsCode() + command);
//        System.out.println("CODE EVALUATED : " +distributedNode.getRuntimeJsCode() + distributedNode.getEvaluationJsCode());
        //evaluate(runtimeJsCode + evaluationJsCode)
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
