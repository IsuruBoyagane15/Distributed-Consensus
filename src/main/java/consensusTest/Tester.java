package consensusTest;

import distributedConsensus.ConsumerGenerator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;
import java.util.UUID;

public class Tester {

    static {
        System.setProperty("test_run_id", java.time.LocalDateTime.now().toString());
    }

    private final String kafkaServerAddress;
    private final Context jsContext;
    private final int maxProcessCount;
    private final String initialJsCose;
    private final String evaluationJsCode;
    private String runtimeJsCode;
    private String immortalProcess;
    private KafkaConsumer<String, String> kafkaConsumer;
    private String jarConfig;
    private final String kafkaTopic;
    private boolean terminate;
    private HashMap<String, Process> activeProcesses;
    private boolean maxProcessCountReached;

    public Tester(String jarLocation, String kafkaServerAddress, String kafkaTopic, int maxProcessCount){
        this.jarConfig = jarLocation;
        this.kafkaTopic = kafkaTopic;
        this.kafkaServerAddress = kafkaServerAddress;
        this.kafkaConsumer = ConsumerGenerator.generateConsumer(kafkaServerAddress, kafkaTopic, "tester");
        this.jsContext = Context.create("js");
        this.immortalProcess = null;
        this.activeProcesses = new HashMap<String, Process>();
        this.terminate = false;
        this.maxProcessCountReached = false;
        this.maxProcessCount = maxProcessCount;
        this.initialJsCose = "var nodeRanks = [];result = {consensus:false, value:null, firstCandidate : null, timeout : false};";
        this.evaluationJsCode = "if(Object.keys(nodeRanks).length != 0){" +
                                    "result.firstCandidate = nodeRanks[0].client;" +
                        "}" +
                        "if(result.timeout){" +
                                    "result.consensus=true;" +
                                    "var leader = null;"+
                                    "var maxRank = 0;"+
                                    "for (var i = 0; i < nodeRanks.length; i++) {"+
                                        "if(nodeRanks[i].rank > maxRank){"+
                                            "result.value = nodeRanks[i].client;" +
                                            "maxRank = nodeRanks[i].rank;" +
                                        "}" +
                                    "}" +
                                "}" +
                        "result;";
        this.runtimeJsCode = initialJsCose;
    }

    public void read(){
            Runnable consuming = () -> {
                int roundNumber = -1;
                try {
                    while (!terminate) {
                        ConsumerRecords<String, String> records = kafkaConsumer.poll(10);
                        for (ConsumerRecord<String, String> record : records) {
                            if (!record.value().startsWith("CHECK")){
                                String[] recordContent = record.value().split(",", 2);
                                int recordNumber = Integer.parseInt(recordContent[0]);
                                String recordMessage = recordContent[1];
                                if(!recordMessage.startsWith("ALIVE")){
                                    if (recordNumber > roundNumber){
                                        roundNumber = recordNumber;
                                        this.immortalProcess = jsContext.eval("js","result = {timeout : false}; var nodeRanks = [];" + recordMessage + "nodeRanks[0].client;").toString();
                                        System.out.println("Cannot kill " + this.immortalProcess);

                                        runtimeJsCode = initialJsCose + recordMessage;
                                    }
                                    else{
                                        if (recordMessage.equals("result.timeout = true;")){
                                            System.out.println("Can kill " + this.immortalProcess);
                                            this.immortalProcess = null;
                                        }
                                        runtimeJsCode = runtimeJsCode + recordMessage;
                                        Value result = jsContext.eval("js",runtimeJsCode + evaluationJsCode);
                                        boolean leaderElected = result.getMember("consensus").asBoolean();
                                        if (leaderElected){
                                            System.out.println("Leader for round number :" + roundNumber + " is " + result.getMember("value"));
                                        }
                                    }
                                }
                            }
                        }
                    }
                } catch(Exception exception) {
                    System.out.println(exception);
                }finally {
                    kafkaConsumer.close();
                }
            };
            new Thread(consuming).start();
    }

    public void startNewProcess(String jarLocation, String kafkaServerAddress, String kafkaTopic){
        String nodeId = UUID.randomUUID().toString();
        System.setProperty("id", nodeId);
        System.out.println(java.time.LocalTime.now() + " :: Id of the new process : " + nodeId);
            ProcessBuilder processBuilder = new ProcessBuilder("java", "-jar", "-Did=" + nodeId, "-Dtest_run_id="+ System.getProperty("test_run_id"), "-Dpath="+System.getProperty("path"), jarLocation, nodeId, kafkaServerAddress, kafkaTopic);
        try {
            Process process = processBuilder.start();
            this.activeProcesses.put(nodeId,process);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void killProcess(){
        Object[] nodeIds = activeProcesses.keySet().toArray();
        Object nodeId = nodeIds[new Random().nextInt(nodeIds.length)];
        System.out.println("Id of the process to be killed  : " + nodeId);

        if (nodeId.equals(this.immortalProcess)){
            System.out.println("Can't kill " + nodeId);
        }
        else{
            Process processToBeKilled = activeProcesses.get(nodeId);
            processToBeKilled.destroy();
            activeProcesses.remove(nodeId);
        }
    }

    public static void main(String[] args){
        Tester tester = new Tester(args[0], args[1], args[2], Integer.parseInt(args[3]));
        tester.read();

        tester.startNewProcess(tester.jarConfig, tester.kafkaServerAddress, tester.kafkaTopic);

        try {
            Thread.sleep((int)(1 + Math.random()*4)*1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        double factor = 0.25;
        while(!tester.terminate){

            double rand = Math.random();
            if(rand> factor){
                if(!tester.maxProcessCountReached){
                    tester.startNewProcess(tester.jarConfig, tester.kafkaServerAddress, tester.kafkaTopic);
                    if( tester.activeProcesses.size() == tester.maxProcessCount){
                        tester.maxProcessCountReached = true;
                        factor = 0.75;
                    }
                }
                else{
                    System.out.println("Maximum Process count: " + tester.activeProcesses.size() + " is achieved.No more processes will start");
                }
            }
            else{
                tester.killProcess();
            }

            int randWait = (int)(1 + Math.random()*4)*1000;
            try {
                Thread.sleep(randWait);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            int count = 0;
            for (HashMap.Entry<String, Process> entry : tester.activeProcesses.entrySet()){
                if(entry.getValue().isAlive()){
                    count++;
                }
                else{
                    tester.activeProcesses.remove(entry);
                }
            }
            if (count == 0){
                tester.terminate = true;
            }

            System.out.println(java.time.LocalTime.now() + " :: Number of active processes : " + count);
        }
    }
}