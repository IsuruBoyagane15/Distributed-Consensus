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
        DistributedConsensus dcf = DistributedConsensus.getDistributeConsensus(this);
        for (int i=0; i<10; i++){
            System.out.println(this.getNodeId() + " is holding lock.");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        dcf.writeACommand("lockStatuses.delete(\""+ this.getNodeId() + "\"" + ");");
        this.setTerminate(true);
    }

    public void start(){
        DistributedConsensus distributedConsensus = DistributedConsensus.getDistributeConsensus(this);
        Runnable consuming = () -> {
            try {
                while (!terminate) {
                    ConsumerRecords<String, String> records = distributedConsensus.getMessages();
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println(record.value());
                        Value result = distributedConsensus.evaluateJsCode(record.value());
                        boolean consensusAchieved = this.checkConsensus(result);
                        if (consensusAchieved) {
                            this.onConsensus(result);
                        }
                    }
                }
            } catch(Exception exception) {
                LOGGER.error("Exception occurred :", exception);
                System.out.println(exception);
            }finally {
                distributedConsensus.closeConsumer();
            }
        };
        new Thread(consuming).start();
    }

    public void setTerminate(boolean terminate){
        this.terminate = terminate;
    }

    public static void handleLock(String nodeId, String kafkaServerAddress, String kafkaTopic){
            LockHandler lockHandler = new LockHandler(nodeId, "var lockStatuses = new Set([]); result = false;",
            "console.log(\"queue is :\" + Array.from(lockStatuses));" +
                    "if(Array.from(lockStatuses)[0] === \"" + nodeId + "\"){" +
                    "result = true;" +
                    "}" +
                    "result;", kafkaServerAddress, kafkaTopic);

        System.out.println("My id is " + lockHandler.getNodeId());
        DistributedConsensus consensusFramework = DistributedConsensus.getDistributeConsensus(lockHandler);
        lockHandler.start();
        consensusFramework.writeACommand("lockStatuses.add(\""+  lockHandler.getNodeId() + "\"" + ");");
    }

    public static void main(String[] args){
        String Id = UUID.randomUUID().toString();
        LockHandler.handleLock(Id, args[0], args[1]);
    }
}
