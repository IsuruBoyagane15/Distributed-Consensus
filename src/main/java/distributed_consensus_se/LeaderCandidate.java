package distributed_consensus_se;

import org.graalvm.polyglot.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class LeaderCandidate extends ConsensusApplication{
    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderCandidate.class);
    private Thread listeningThread;
    private String electedLeader;


    public LeaderCandidate(String nodeId, String runtimeJsCode, String evaluationJsCode, String kafkaServerAddress,
                           String kafkaTopic) {
        super(nodeId, runtimeJsCode, evaluationJsCode, kafkaServerAddress, kafkaTopic);
        this.listeningThread = null;
        this.electedLeader = null;
    }

    @Override
    public void handleHeartbeat(String sender) {
        if (sender.equals(this.electedLeader) && !this.electedLeader.equals(getNodeId())) {
//            System.out.println("GOT HB" );
            System.out.println("GOT HB FROM " + this.electedLeader);
            this.listeningThread.interrupt();
        }
    }

    public String getElectedLeader() {
        return electedLeader;
    }

    @Override
    public boolean onReceiving(Value result) {
        return result.getMember("consensus").asBoolean();
    }

    @Override
    public void commitAgreedValue(Value value) {
        DistributedConsensus dcf = DistributedConsensus.getDistributeConsensus(this);
        if (this.electedLeader == null){

            this.electedLeader = value.getMember("value").asString();

            System.out.println("LEADER IS : " + getElectedLeader());
            if (this.electedLeader.equals(this.getNodeId())){
                this.startHeartbeatSender();
            }
            else{
                this.startHeartbeatListener();
            }
        }
    }

    public void startHeartbeatSender(){
        System.out.println("STARTING HEARTBEAT SENDER.");
        while (true) {
            System.out.println("ALIVE");
            DistributedConsensus consensusFramework = DistributedConsensus.getDistributeConsensus(this);
            consensusFramework.writeACommand("ALIVE," + getNodeId());
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                LOGGER.error("LEADER : " + getNodeId() + " is interrupted.");
                e.printStackTrace();
            }
        }
    }

    public void startHeartbeatListener(){
        System.out.println("STARTING HEARTBEAT LISTENER.");
        this.listeningThread = new Thread(new HeartbeatListener(this));
        this.listeningThread.start();
        LOGGER.info("FOLLOWER :" + getNodeId() + " started listening to heartbeats.");
    }

    public static void electLeader(String nodeId, String kafkaServerAddress, String kafkaTopic){

        LeaderCandidate leaderCandidate = new LeaderCandidate(nodeId, "var nodeRanks = [];result = {consensus:false, value:null, hb:null};",

                        "if (result.value == null){" +
                                            "var maxRank = 0;" +
                                            "for (var i = 0; i < nodeRanks.length; i++) {" +
                                            "    if(nodeRanks[i].rank > maxRank){" +
                                            "        result.value = nodeRanks[i].client;" +
                                            "        result.consensus = true;" +
//                                            "        nodeRanks.pop(nodeRanks[i]);" +
                                            "        maxRank = nodeRanks[i].rank;" +
                                            "    }" +
                                            "}" +
                                        "}" +
                                        "result",
                kafkaServerAddress, kafkaTopic);

        System.out.println("My id is " + leaderCandidate.getNodeId() + "\n");
        DistributedConsensus consensusFramework = DistributedConsensus.getDistributeConsensus(leaderCandidate);
        consensusFramework.start();
        int nodeRank = (int)(1 + Math.random()*100);
        consensusFramework.writeACommand("if (result.value == null){" +
                "    result.value = \"" + leaderCandidate.getNodeId() +"\";" +
                "result.consensus = true;" +
                "}" +
                "else{" +
                "nodeRanks.push({client:\""+ leaderCandidate.getNodeId() + "\",rank:" + nodeRank + "});" +
                "}");
    }



    public void startNewRound(){
        this.electedLeader = null;
        this.listeningThread = null;
        DistributedConsensus dcf = DistributedConsensus.getDistributeConsensus(this);
        dcf.writeACommand("result = {consensus:false, value:null};nodeRanks = nodeRanks.filter(function( obj ) {" +
                "    return obj.client !== \"" + electedLeader + "\";" +
                "  });");
    }

    public static void main(String[] args){
        String id = UUID.randomUUID().toString();
        LeaderCandidate.electLeader(id, args[0], args[1]);

    }
}