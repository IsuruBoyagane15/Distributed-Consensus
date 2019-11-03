//package Test;
//
//import distributed_consensus_se.DistributedConsensus;
//import org.apache.commons.codec.digest.DigestUtils;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.graalvm.polyglot.Value;
//
//public class TopicTest {
//    public void startRound(){
//
//        String rawString = generateUniqueKey();
//        String unique_round_key = DigestUtils.sha256Hex(rawString);
//        final String checkRecord = "CHECK,"+ unique_round_key;
//
////        System.out.println("My ID ID " + distributedNode.getNodeId() + " :: " + "CHECK MESSAGE IS " + checkRecord);
//        writeACommand(checkRecord);
//
//
//        Runnable consuming = () -> {
//            boolean correctRoundIdentified = false;
//
//            String latestRoundsJsCode = "";
//            int latestRoundNumber = 0;
//
//            String nextRoundCode;
//            int nextRoundNumber = 0;
//            DistributedConsensus.roundStatuses nextRoundStatus;
//
//            try {
//                while (!terminate) {
//                    ConsumerRecords<String, String> records = kafkaConsumer.poll(10);
//                    for (ConsumerRecord<String, String> record : records) {
//                        if (!correctRoundIdentified){
//                            //IDENTIFYING THE ROUND
//                            if (record.value().equals(checkRecord)) {
//                                //take decision on round and state AFTER CHECHRECORD IS FOUND
////                                System.out.println("UNIQUE HASH IS FOUND :: IDENTIFYING ROUND TO PARTICIPATE...");
//                                if (latestRoundsJsCode == ""){
////                                    System.out.println("TOPIC IS EMPTY :: STARTING NEW ROUND WITH ROUND_NUMBER = 0 ...");
//                                    nextRoundStatus = DistributedConsensus.roundStatuses.NEW;
//                                    nextRoundCode = "";
//                                }
//                                else{
//                                    Value latestRoundResult = evaluateJsCode(latestRoundsJsCode);
//                                    boolean consensusAchieved = distributedNode.onReceiving(latestRoundResult);
//                                    if (consensusAchieved){
////                                        System.out.println("PREVIOUS ROUND IS FINISHED :: STARTING NEW ROUND WITH ROUND_NUMBER = 0 ...");
//                                        nextRoundStatus = DistributedConsensus.roundStatuses.FINISHED;
//                                        nextRoundCode = "";
//                                        nextRoundNumber = latestRoundNumber;
//                                    }
//                                    else{
////                                        System.out.println("ONGOING ROUND :: PASS THIS ROUND'S NUMBER AND CODES TO CONSENSUS APPLICATION...");
//                                        nextRoundStatus = DistributedConsensus.roundStatuses.ONGOING;
//                                        nextRoundCode = latestRoundsJsCode;
//                                        nextRoundNumber = latestRoundNumber;
//                                    }
//                                }
//                                System.out.println("\nNEXT ROUND STATUS is " + nextRoundStatus);
//                                System.out.println("NEXT ROUND NUMBER is " + nextRoundNumber);
//                                System.out.println("NEXT ROUND JSCODE is " + nextRoundCode + "\n");
//
//
//                                distributedNode.participate(nextRoundStatus,nextRoundNumber,nextRoundCode);
////                                System.out.println("ROUND IDENTIFICATION IS FINISHED...");
//                                correctRoundIdentified = true;
//
//                            }
//                            else if (!record.value().startsWith("CHECK,")){
//                                //collect most recent rounds codes
//
//                                String[] recordContent = record.value().split(",", 2);
//                                int recordRoundNumber = Integer.parseInt(recordContent[0]);
//                                String recordMessage = recordContent[1]; //ALIVE or clean JS
//                                if (!recordMessage.startsWith("ALIVE")){
//                                    if (recordRoundNumber>latestRoundNumber){
//                                        latestRoundsJsCode = recordMessage;
//                                        latestRoundNumber = recordRoundNumber;
//                                    }
//                                    else if(recordRoundNumber==latestRoundNumber){
//                                        latestRoundsJsCode += recordMessage;
//                                    }
//                                }
//
//                            }
//
//                        }
//                        else if (!record.value().startsWith("CHECK,")){
//                            //EVALUATING RECORDS OF CURRENT ROUND
//                            String[] recordContent = record.value().split(",", 2);
//                            String recordMessage = recordContent[1]; //ALIVE,nodeId or clean JS
//
//                            if(recordMessage.startsWith("ALIVE")){
//                                distributedNode.handleHeartbeat();
//                            }
//                            else{
//                                Value result = evaluateJsCode(recordMessage);
////                                System.out.println("RESULT IS : " + result);
//                                boolean consensusAchieved = distributedNode.onReceiving(result);
//                                if (consensusAchieved) {
//                                    distributedNode.onConsensus(result);
//                                }
//                            }
//
//                        }
//                    }
//                }
//            } catch(Exception exception) {
//                LOGGER.error("Exception occurred :", exception);
//                System.out.println(exception);
//            }finally {
//                kafkaConsumer.close();
//            }
//        };
//        new Thread(consuming).start();
//    }
//    public String generateUniqueKey(){
//        String ALPHA_NUMERIC_STRING = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
//        StringBuilder builder = new StringBuilder();
//        int rawStringLength = 16;
//        while (rawStringLength-- != 0) {
//            int character = (int)(Math.random()*ALPHA_NUMERIC_STRING.length());
//            builder.append(ALPHA_NUMERIC_STRING.charAt(character));
//        }
//        return builder.toString();
//    }
//
//    public void writeACommand(String command) {
//        kafkaProducer.send(new ProducerRecord<String, String>(distributedNode.getKafkaTopic(), command));
//    }
//}
