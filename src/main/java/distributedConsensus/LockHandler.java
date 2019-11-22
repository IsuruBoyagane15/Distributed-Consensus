package distributedConsensus;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.log4j.Logger;
import org.graalvm.polyglot.Value;
import java.util.UUID;

public class LockHandler extends ConsensusApplication{
    private static final Logger LOGGER = Logger.getLogger(LeaderCandidate.class);
    private boolean terminate;

    public LockHandler(String nodeId, String runtimeJsCode, String evaluationJsCode, String kafkaServerAddress, String kafkaTopic) {
        super(nodeId, runtimeJsCode, evaluationJsCode, kafkaServerAddress, kafkaTopic);
        this.terminate = false;
    }

    @Override
    public boolean checkConsensus(Value result) {
        return result.asBoolean();
    }

    @Override
    public void onConsensus(Value value) {
        for (int i=0; i<10; i++){
            LOGGER.info(this.getNodeId() + " is holding lock.");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        this.distributedConsensus.writeACommand("lockStatuses.delete(\""+ this.getNodeId() + "\"" + ");");
        this.setTerminate(true);
    }

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

    public void setTerminate(boolean terminate){
        this.terminate = terminate;
    }

    public static void main(String[] args){
        String nodeId = UUID.randomUUID().toString();
        LockHandler lockHandler = new LockHandler(nodeId, "var lockStatuses = new Set([]); result = false;",
                "console.log(\"queue is :\" + Array.from(lockStatuses));" +
                        "if(Array.from(lockStatuses)[0] === \"" + nodeId + "\"){" +
                        "result = true;" +
                        "}" +
                        "result;", args[0], args[1]);

        LOGGER.info("My id is " + lockHandler.getNodeId());
        lockHandler.start();
        lockHandler.distributedConsensus.writeACommand("lockStatuses.add(\""+  lockHandler.getNodeId() + "\"" + ");");
    }
}
