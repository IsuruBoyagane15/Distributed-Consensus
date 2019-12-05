package distributedLock;

import distributedConsensus.ConsensusApplication;
import leaderElection.LeaderCandidate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.log4j.Logger;
import org.graalvm.polyglot.Value;
import java.util.UUID;

/**
 * Java NODE trying to acquire distributed lock
 * Can acquire the lock, release the lock
 */
public class LockHandler extends ConsensusApplication {
    private static final Logger LOGGER = Logger.getLogger(LeaderCandidate.class);
    private boolean terminate;

    /**
     *Constructor
     *
     * @param nodeId unique id to identify the LockHandler
     * @param runtimeJsCode String containing Javascript records
     * @param evaluationJsCode Javascript logic to evaluate and decide whether lock can be acquired or not
     * @param kafkaServerAddress URL of Kafka server
     * @param kafkaTopic Kafka topic to subscribe to participate to achieving distributed lock
     */
    public LockHandler(String nodeId, String runtimeJsCode, String evaluationJsCode,
                       String kafkaServerAddress, String kafkaTopic) {
        super(nodeId, runtimeJsCode, evaluationJsCode, kafkaServerAddress, kafkaTopic);
        this.terminate = false;
    }

    /**
     * Check whether consensus is achieved or not based on the evaluation Javascript logic
     *
     * @param result Value return by Javascript evaluation
     * @return Whether this LockHandler acquired the lock or not
     */
    @Override
    public boolean checkConsensus(Value result) {
        return result.asBoolean();
    }

    /**
     * Action taken after acquiring lock
     *
     * @param value Whether this LockHandler acquired the lock or not
     */
    @Override
    public void onConsensus(Value value) {
        for (int i=0; i<10; i++){
            LOGGER.info(nodeId + " is holding lock.");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        this.distributedConsensus.writeACommand("lockStatuses.delete(\""+ nodeId + "\"" + ");");
        this.setTerminate(true);
    }

    /**
     * Read and evaluate Kafka records in a separate thread
     */
    public void start(){
        Runnable consuming = () -> {
            try {
                while (!terminate) {
                    ConsumerRecords<String, String> records = this.distributedConsensus.getMessages();
                    for (ConsumerRecord<String, String> record : records) {
                        Value result = this.distributedConsensus.evaluateJsCode(record.value());
                        boolean consensusAchieved = this.checkConsensus(result);
                        if (consensusAchieved) {
                            this.onConsensus(result);
                        }
                    }
                }
            } catch(Exception exception) {
                LOGGER.error("Exception occurred :", exception);
            }finally {
                this.distributedConsensus.closeConsumer();
            }
        };
        new Thread(consuming).start();
    }

    /**
     * Setting terminate to true will stop the kafkaConsumer and terminate the LockHandler
     *
     * @param terminate whether to terminate or not
     */
    public void setTerminate(boolean terminate){
        this.terminate = terminate;
    }

    /**
     * Instantiate the LockHandler and participate to consensus process
     * @param args Kafka server location and Kafka topic
     */
    public static void main(String[] args){
        String nodeId = UUID.randomUUID().toString();
        LockHandler lockHandler = new LockHandler(nodeId, "var lockStatuses = new Set([]); result = false;",
                "console.log(\"queue is :\" + Array.from(lockStatuses));" +
                        "if(Array.from(lockStatuses)[0] === \"" + nodeId + "\"){" +
                        "result = true;" +
                        "}" +
                        "result;", args[0], args[1]);

        LOGGER.info("My id is " + lockHandler.nodeId);
        lockHandler.start();
        lockHandler.distributedConsensus.writeACommand("lockStatuses.add(\""+  lockHandler.nodeId + "\"" + ");");
    }
}
