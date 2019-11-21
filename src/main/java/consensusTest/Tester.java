package consensusTest;

import distributedConsensus.ConsumerGenerator;
import distributedConsensus.LeaderCandidate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;

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
    private HashMap<String, LeaderCandidate> activeProcesses;
    private boolean maxProcessCountReached;

    public Tester(String jarLocation, String kafkaServerAddress, String kafkaTopic, int maxProcessCount){
        this.jarConfig = jarLocation;
        this.kafkaTopic = kafkaTopic;
        this.kafkaServerAddress = kafkaServerAddress;
        this.kafkaConsumer = ConsumerGenerator.generateConsumer(kafkaServerAddress, kafkaTopic, "tester");
        this.jsContext = Context.create("js");
        this.immortalProcess = null;
        this.activeProcesses = new HashMap<String, LeaderCandidate>();
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
                                        System.out.println(java.time.LocalTime.now() + " :: Cannot kill " + this.immortalProcess);

                                        runtimeJsCode = initialJsCose + recordMessage;
                                    }
                                    else{
                                        if (recordMessage.equals("result.timeout = true;")){
                                            System.out.println(java.time.LocalTime.now() + " :: Can kill " + this.immortalProcess);
                                            this.immortalProcess = null;
                                        }
                                        runtimeJsCode = runtimeJsCode + recordMessage;
                                        Value result = jsContext.eval("js",runtimeJsCode + evaluationJsCode);
                                        boolean leaderElected = result.getMember("consensus").asBoolean();
                                        if (leaderElected){
                                            System.out.println(java.time.LocalTime.now() + " :: Leader for round number :" + roundNumber + " is " + result.getMember("value"));
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

        LeaderCandidate leaderCandidate = new LeaderCandidate(nodeId, "var nodeRanks = [];result = {consensus:false, value:null, firstCandidate : null, timeout : false};",

                "if(Object.keys(nodeRanks).length != 0){" +
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
                        "result;",
                kafkaServerAddress, kafkaTopic);

        Thread leaderCandidateThread = new Thread(leaderCandidate);
        leaderCandidateThread.setName(nodeId + "_main");
        leaderCandidateThread.start();
        this.activeProcesses.put(nodeId, leaderCandidate);
    }

    public void killProcess(){
        Object[] nodeIds = activeProcesses.keySet().toArray();
        Object nodeId = nodeIds[new Random().nextInt(nodeIds.length)];
        System.out.println(java.time.LocalTime.now() + " :: Id of the process to be killed  : " + nodeId);

        if (nodeId.equals(this.immortalProcess)){
            System.out.println(java.time.LocalTime.now() + " :: Can't kill " + nodeId);
        }
        else{
            LeaderCandidate leaderCandidateToBeKilled = activeProcesses.get(nodeId);
            activeProcesses.remove(nodeId);
            leaderCandidateToBeKilled.setTerminate(true);
            if(activeProcesses.size() == 0){
                this.terminate = true;
            }
        }
    }

    public static void main(String[] args){
        Tester tester = new Tester(args[0], args[1], args[2], Integer.parseInt(args[3]));
        tester.read();

        tester.startNewProcess(tester.jarConfig, tester.kafkaServerAddress, tester.kafkaTopic);

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        tester.killProcess();
    }
}