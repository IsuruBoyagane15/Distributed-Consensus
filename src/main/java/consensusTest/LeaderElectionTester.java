package consensusTest;

import distributedConsensus.ConsumerGenerator;
import leaderElection.LeaderCandidate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;

import java.util.HashMap;
import java.util.Random;
import java.util.UUID;

/**
 * External class to start/kill LeaderCandidate threads and monitor the execution of leader elections
 */
public class LeaderElectionTester {

    private static final Logger LOGGER = Logger.getLogger(LeaderCandidate.class);

    private final String kafkaServerAddress, kafkaTopic,  initialJsCode, evaluationJsCode;
    private final Context jsContext;
    private String runtimeJsCode, immortalProcess;
    private KafkaConsumer<String, String> kafkaConsumer;
    private boolean terminate; //, maxProcessCountReached;
    private HashMap<String, LeaderCandidate> activeProcesses;

    /**
     * Constructor
     *
     * @param kafkaServerAddress URL of Kafka server
     * @param kafkaTopic Kafka topic which LeaderCandidates communicate through
     */
    public LeaderElectionTester(String kafkaServerAddress, String kafkaTopic){ //, int maxProcessCount
        this.kafkaTopic = kafkaTopic;
        this.kafkaServerAddress = kafkaServerAddress;
        this.kafkaConsumer = ConsumerGenerator.generateConsumer(kafkaServerAddress, kafkaTopic, "tester");
        this.jsContext = Context.create("js");
        this.immortalProcess = null;
        this.activeProcesses = new HashMap<>();
        this.terminate = false;
        this.initialJsCode = "var nodeRanks = [];result = {consensus:false, value:null, firstCandidate : null, timeout : false};";
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
        this.runtimeJsCode = initialJsCode;
    }

    /**
     * Consume the same Kafka log in which leader election happens and extract special states
     * in the election process
     */
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
                                    LOGGER.info("Cannot kill " + this.immortalProcess + " for a while");
                                    runtimeJsCode = initialJsCode + recordMessage;
                                }
                                else{
                                    if (recordMessage.equals("result.timeout = true;")){
                                        LOGGER.info(this.immortalProcess + " can be killed from now on");
                                        this.immortalProcess = null;
                                    }
                                    runtimeJsCode = runtimeJsCode + recordMessage;
                                    Value result = jsContext.eval("js",runtimeJsCode + evaluationJsCode);
                                    boolean leaderElected = result.getMember("consensus").asBoolean();
                                    if (leaderElected){
                                        LOGGER.info("Leader for round number :" + roundNumber + " is " + result.getMember("value"));
                                    }
                                }
                            }
                        }
                    }
                }
            } catch(Exception exception) {
                LOGGER.error(exception.getStackTrace());
            }finally {
                kafkaConsumer.close();
            }
        };
        Thread consumer = new Thread(consuming);
        consumer.setName("tester_consumer");
        new Thread(consuming).start();
    }

    /**
     * Start a new LeaderCandidate thread
     *
     * @param kafkaServerAddress URL of Kafka server
     * @param kafkaTopic Kafka topic which LeaderCandidates communicate through
     */
    public void startNewProcess(String kafkaServerAddress, String kafkaTopic){
        String nodeId = UUID.randomUUID().toString();
        System.setProperty("id", nodeId);
        LOGGER.info("Id of the new process : " + nodeId);

        LeaderCandidate leaderCandidate = new LeaderCandidate(nodeId, initialJsCode, this.evaluationJsCode,
                kafkaServerAddress, kafkaTopic);

        Thread leaderCandidateThread = new Thread(leaderCandidate);
        leaderCandidateThread.setName(nodeId + "_consumer");
        leaderCandidateThread.start();
        this.activeProcesses.put(nodeId, leaderCandidate);
    }

    /**
     * stop a LeaderCandidate thread
     */
    public void killProcess(){
        Object[] nodeIds = activeProcesses.keySet().toArray();
        Object nodeId = nodeIds[new Random().nextInt(nodeIds.length)];
        LOGGER.info("Id of the process to be killed  : " + nodeId + " :: " + "Id of the immortal process : " + this.immortalProcess);
        if (nodeId.equals(this.immortalProcess)){
            LOGGER.info("Can't kill " + nodeId + " at this moment; Trying to kill again");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        else{
            LeaderCandidate leaderCandidateToBeKilled = activeProcesses.get(nodeId);
            activeProcesses.remove(nodeId);
            leaderCandidateToBeKilled.setTerminate(true);
            LOGGER.info("Killed " + nodeId);
        }
    }

    /**
     * Start n threads.
     * Randomly start/kill threads maintaining at least n*0.8 threads and at most n*1.2 in the election.
     * Continue step 2 for testTime time period.
     * Kill all the remaining threads to finish the test run.
     *
     * @param args kafkaServerAddress, KafkaTopic, maxProcessCount testTime
     */
    public static void main(String[] args){
        Thread.currentThread().setName("tester_main");
        int testSeconds = Integer.parseInt(args[3]);
        int maxProcessCount = Integer.parseInt(args[2]);
        LeaderElectionTester leaderElectionTester = new LeaderElectionTester(args[0], args[1]);
        leaderElectionTester.read();

        for (int i = 0; i < maxProcessCount*0.8; i++){
            leaderElectionTester.startNewProcess(leaderElectionTester.kafkaServerAddress, leaderElectionTester.kafkaTopic);
            int randWait = (int) (1 + Math.random() * 10) * 1000;
            try {
                Thread.sleep(randWait);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        long startSeconds = System.currentTimeMillis();
        while(System.currentTimeMillis() - startSeconds <= testSeconds*1000) {
            double random = Math.random();
            if (random > 0.5) {
                if (leaderElectionTester.activeProcesses.size() < maxProcessCount*1.2) {
                    leaderElectionTester.startNewProcess(leaderElectionTester.kafkaServerAddress, leaderElectionTester.kafkaTopic);
                }
                else{
                    leaderElectionTester.killProcess();
                }
            }
            else {
                if (leaderElectionTester.activeProcesses.size() > maxProcessCount*0.8){
                    leaderElectionTester.killProcess();
                }
                else{
                    leaderElectionTester.startNewProcess(leaderElectionTester.kafkaServerAddress, leaderElectionTester.kafkaTopic);
                }
            }
            LOGGER.info("Number of leader candidates alive : " + leaderElectionTester.activeProcesses.size());

            int randWait = (int) (1 + Math.random() * 4) * 1000;
            try {
                Thread.sleep(randWait);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        LOGGER.info("TestTime is out. Will kill all the threads and finish the test run");

        while(leaderElectionTester.activeProcesses.size() > 0){
            leaderElectionTester.killProcess();
        }
        leaderElectionTester.terminate = true;
        LOGGER.info("Test run is finished successfully");
    }
}
