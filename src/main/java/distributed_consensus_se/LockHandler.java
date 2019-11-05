package distributed_consensus_se;

import org.graalvm.polyglot.Value;

import java.util.UUID;

public class LockHandler extends ConsensusApplication{
    public LockHandler(String nodeId, String runtimeJsCode, String evaluationJsCode, String kafkaServerAddress, String kafkaTopic) {
        super(nodeId, runtimeJsCode, evaluationJsCode, kafkaServerAddress, kafkaTopic);
    }

    @Override
    public void handleHeartbeat() {

    }

    @Override
    public boolean checkConsensus(Value result) {
        return result.asBoolean();
    }

    @Override
    public boolean onReceiving(Value value) {
        return checkConsensus(value);
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
        dcf.setTerminate(true);
    }

//    @Override
    public void participate(DistributedConsensus.roundStatuses nextRoundStatus, int nextRoundNumber, String nextRoundCode) {
    }

    public static void handleLock(String nodeId, String kafkaServerAddress, String kafkaTopic){
            LockHandler lockHandler = new LockHandler(nodeId, "var lockStatuses = new Set([]); result = false;",
//            "console.log(\"queue is :\" + Array.from(lockStatuses));" +
                    "if(Array.from(lockStatuses)[0] === \"" + nodeId + "\"){" +
                    "result = true;" +
                    "}" +
                            "console.log(\"nodeRanks is :\" + Array.from(nodeRanks));" +
                    "result;", kafkaServerAddress, kafkaTopic);

        System.out.println("My id is " + lockHandler.getNodeId());
        DistributedConsensus consensusFramework = DistributedConsensus.getDistributeConsensus(lockHandler);
        consensusFramework.start();
        consensusFramework.writeACommand("lockStatuses.add(\""+  lockHandler.getNodeId() + "\"" + ");");
    }

    public static void main(String[] args){
        String Id = UUID.randomUUID().toString();
        LockHandler.handleLock(Id, args[0], args[1]);
    }
}
