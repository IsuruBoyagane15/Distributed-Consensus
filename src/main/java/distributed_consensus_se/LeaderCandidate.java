package distributed_consensus_se;

import org.graalvm.polyglot.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class LeaderCandidate extends ConsensusApplication{
    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderCandidate.class);
    private boolean timeoutCounted;
    private int roundNumber;
    private Thread listeningThread;
    private String electedLeader;
    private final String initialJsCode;


    public LeaderCandidate(String nodeId, String runtimeJsCode, String evaluationJsCode, String kafkaServerAddress,
                           String kafkaTopic) {
        super(nodeId, runtimeJsCode, evaluationJsCode, kafkaServerAddress, kafkaTopic);
        this.initialJsCode = runtimeJsCode;
        this.listeningThread = null;
        this.electedLeader = null;
        this.timeoutCounted = false;
    }

    public void setElectedLeader(String electedLeader) {
        this.electedLeader = electedLeader;
    }

    @Override
    public void participate(DistributedConsensus.roundStatuses myState, int myRoundNumber, String myRoundCodes) {
        int nodeRank = (int)(1 + Math.random()*100);
        DistributedConsensus consensusFramework = DistributedConsensus.getDistributeConsensus(this);
        this.roundNumber = myRoundNumber;
        this.setRuntimeJsCode(initialJsCode);


        if (myState == DistributedConsensus.roundStatuses.NEW){
            System.out.println("PARTICIPATED :: MY RANK IS " + nodeRank + " :: " + java.time.LocalTime.now());
            setRuntimeJsCode(initialJsCode);
            consensusFramework.writeACommand(myRoundNumber + ",if(!result.timeout){nodeRanks.push({client:\""+ getNodeId() + "\",rank:" +
                    nodeRank +"});}");
        }

        else if(myState == DistributedConsensus.roundStatuses.ONGOING){
            System.out.println("PARTICIPATED :: MY RANK IS " + nodeRank + " :: "  + java.time.LocalTime.now());
            setRuntimeJsCode(initialJsCode + myRoundCodes);
            consensusFramework.writeACommand(myRoundNumber + ",if(!result.timeout){nodeRanks.push({client:\""+ getNodeId() + "\",rank:" +
                    nodeRank +"});}");
        }

        else if(myState == DistributedConsensus.roundStatuses.FINISHED){

            if(this.listeningThread == null){
                System.out.println("WAITING FOR HBs :: WILL JOIN TO NEXT ROUND" + " :: " +  java.time.LocalTime.now());
                this.listeningThread = new Thread(new HeartbeatListener(this));
                this.listeningThread.start();
                try {
                    listeningThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public boolean onReceiving(Value result) {
        DistributedConsensus dcf = DistributedConsensus.getDistributeConsensus(this);
        if(electedLeader == null){
            System.out.println("FIST MEMBER IS " + result.getMember("firstCandidate").toString());
            if (result.getMember("firstCandidate").toString().equals(getNodeId()) && !timeoutCounted){
                try {
                    Thread.sleep(4000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                this.timeoutCounted = true;
                dcf.writeACommand(this.roundNumber + ",result.timeout = true;");
                System.out.println("WROTE TIMEOUT " + ":: " +  java.time.LocalTime.now());
            }
            return result.getMember("consensus").asBoolean();
        }
        else{
            return false;
        }
    }

    @Override
    public void onConsensus(Value value) {
        this.electedLeader = value.getMember("value").toString();
        if (value.getMember("value").toString().equals(getNodeId())) {
            this.startHeartbeatSender();
        }
        else{
            this.startHeartbeatListener();
        }
    }

    public void startHeartbeatSender(){
        while (true) {
            System.out.println("ALIVE " + ":: " +  java.time.LocalTime.now());
            DistributedConsensus consensusFramework = DistributedConsensus.getDistributeConsensus(this);
            consensusFramework.writeACommand(roundNumber + ",ALIVE");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                LOGGER.error("LEADER : " + getNodeId() + " is interrupted.");
                e.printStackTrace();
            }
        }
    }

    public void startHeartbeatListener(){
        this.listeningThread = new Thread(new HeartbeatListener(this));
        this.listeningThread.start();
        LOGGER.info("FOLLOWER :" + getNodeId() + " started listening to heartbeats.");
    }

    @Override
    public void handleHeartbeat() {
        if (electedLeader != null){
            this.listeningThread.interrupt();
        }
    }

    public void startNewRound(){
            DistributedConsensus dcf = DistributedConsensus.getDistributeConsensus(this);
            this.setRuntimeJsCode("var nodeRanks = [];result = {consensus:false, value:null, firstCandidate : null, timeout : false};");
            electedLeader = null;
            this.timeoutCounted = false;
            roundNumber ++;

            int nodeRank = (int)(1 + Math.random()*100);
            System.out.println("NEW ELECTION :: ROUND NUMBER IS " + roundNumber + " MY RANK IS " + nodeRank + " :: " +  java.time.LocalTime.now());
            dcf.writeACommand(roundNumber + ",if(!result.timeout){nodeRanks.push({client:\""+ getNodeId() + "\",rank:" +
                    nodeRank +"})};");
            LOGGER.info("New leader election is started.");
    }

    public static void electLeader(String nodeId, String kafkaServerAddress, String kafkaTopic){

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
                        "console.log(\"nodeRanks is : \" + JSON.stringify(nodeRanks  ));" +
                        "result;",
                kafkaServerAddress, kafkaTopic);

        DistributedConsensus consensusFramework = DistributedConsensus.getDistributeConsensus(leaderCandidate);
        consensusFramework.startRound();
    }

    public static void main(String[] args){
        String id = UUID.randomUUID().toString();
        LeaderCandidate.electLeader(id, args[0], args[1]);

    }
}