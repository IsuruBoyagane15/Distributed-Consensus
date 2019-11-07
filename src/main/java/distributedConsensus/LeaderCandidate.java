package distributedConsensus;

import org.graalvm.polyglot.Value;
import org.apache.log4j.Logger;

import java.util.UUID;

public class LeaderCandidate extends ConsensusApplication{
    private static final Logger LOGGER = Logger.getLogger(LeaderCandidate.class);
    private boolean skipARound;
    private DistributedConsensus.roundStatuses joiningState;
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
        this.timeoutCounted = false; //FIRST LEADER CANDIDATE TO JOIN THAT ROUND WILL COUNT A TIMEOUT AND CLOSE THE VOTING BY WRITING IT TO KAFKA
        this.joiningState = null;
        this.skipARound = false; //USED BY CANDIDATE WHICH GOT FINISHED STATUS TO INCREMENT ROUND NUMBER WHICH IS SKIP BY WAITING CANDIDATE
    }

    public void setElectedLeader(String electedLeader) {
        this.electedLeader = electedLeader;
    }

    @Override
    public void participate(DistributedConsensus.roundStatuses myState, int proposedRoundNumber, String myRoundCodes) {
        int nodeRank = (int)(1 + Math.random()*100);
        DistributedConsensus consensusFramework = DistributedConsensus.getDistributeConsensus(this);
        this.roundNumber = proposedRoundNumber;
        this.joiningState = myState;
        this.setRuntimeJsCode(initialJsCode);


        if (myState == DistributedConsensus.roundStatuses.NEW){
            System.out.println("PARTICIPATED :: MY RANK IS " + nodeRank + " :: " + java.time.LocalTime.now());
            consensusFramework.writeACommand(proposedRoundNumber + ",if(!result.timeout){nodeRanks.push({client:\""+ getNodeId() + "\",rank:" +
                    nodeRank +"});}");
        }

        else if(myState == DistributedConsensus.roundStatuses.ONGOING){
            System.out.println("PARTICIPATED :: MY RANK IS " + nodeRank + " :: "  + java.time.LocalTime.now());
            setRuntimeJsCode(initialJsCode + myRoundCodes);
            consensusFramework.writeACommand(proposedRoundNumber + ",if(!result.timeout){nodeRanks.push({client:\""+ getNodeId() + "\",rank:" +
                    nodeRank +"});}");
        }

        else if(myState == DistributedConsensus.roundStatuses.FINISHED){
            System.out.println("WAITING FOR HBs :: WILL JOIN TO NEXT ROUND" + " :: " +  java.time.LocalTime.now());
            startHeartbeatListener();
        }
        else{
            LOGGER.error("LeaderCandidate" +
                        " " + getNodeId() + " WRONG STATE for last round");
            System.out.println("ERROR :: WRONG STATE");
        }
    }

    @Override //SHOULD NOT BE CALLED WHEN THERE IS AN IDENTIFIED LEADER
    public boolean onReceiving(Value result) {
        DistributedConsensus dcf = DistributedConsensus.getDistributeConsensus(this);
        if(electedLeader == null){

            if (this.joiningState == DistributedConsensus.roundStatuses.FINISHED){
                if (!skipARound){
                    //to ensure the round number is incremented once for skipping round
                    this.roundNumber ++;
                    skipARound = true;

                }
                System.out.println("SOMEONE HAS STARTED NEW ROUND");
                LOGGER.info("LeaderCandidate of " + getNodeId() + " skipping the round " + roundNumber);
                listeningThread.interrupt();
                return false;
            }

//            System.out.println("FIRST MEMBER IS " + result.getMember("firstCandidate").toString());
            if (result.getMember("firstCandidate").toString().equals(getNodeId()) && !timeoutCounted){
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                this.timeoutCounted = true;
                dcf.writeACommand(this.roundNumber + ",result.timeout = true;");
                LOGGER.info("LeaderCandidate " + getNodeId() + " wrote timeout");
                System.out.println("WROTE TIMEOUT " + ":: " +  java.time.LocalTime.now());
                return false;

            }
            else{
                return checkConsensus(result);
            }
        }
        else{
            LOGGER.error("onReceive() WHEN LEADER IS THERE.");
            System.out.println("ERROR :: onReceive() WHEN LEADER IS THERE.");
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

    @Override
    public void handleHeartbeat() {
        LOGGER.info("LeaderCandidate " + getNodeId() + " found HB");
        this.listeningThread.interrupt();
    }

    @Override
    public boolean checkConsensus(Value result) {
        return result.getMember("consensus").asBoolean();
    }

    public void startHeartbeatSender(){
        LOGGER.info("LeaderCandidate " + getNodeId() + " started sending HB");
        while (true) {
            System.out.println("ALIVE " + ":: " +  java.time.LocalTime.now());
            DistributedConsensus consensusFramework = DistributedConsensus.getDistributeConsensus(this);
            consensusFramework.writeACommand(roundNumber + ",ALIVE,"+ getNodeId());
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                LOGGER.error("LeaderCandidate " + getNodeId() + " was interrupted while sending HB");
                e.printStackTrace();
            }
        }
    }

    public void startHeartbeatListener(){
        LOGGER.info("LeaderCandidate " + getNodeId() + " started listening to HB");
        this.listeningThread = new Thread(new HeartbeatListener(this));
        this.listeningThread.start();

    }

    public void cleanRound(){
        this.setRuntimeJsCode(initialJsCode); // IN EACH NEW ROUND JS SHOULD BE RESET
        this.joiningState = null; //SHOULD BE DONE SINCE "FINISHED" NODES GET INTERRUPTED BY MESSAGES UNTIL THEY CALL THEIR FIRST startNewRound()
        this.timeoutCounted = false;
        this.electedLeader = null;
        LOGGER.info("LeaderCandidate" + getNodeId() + " cleaned round status");
    }

    public void startNewRound(){
            DistributedConsensus dcf = DistributedConsensus.getDistributeConsensus(this);
//            this.setRuntimeJsCode(initialJsCode); // DONE IN CONSUMER LOOP WHEN EVALUATING SECTION FINDS A HIGHER ROUNDNUMBERED RECORD
//            this.joiningState = null; //SHOULD BE DONE SINCE "FINISHED" NODES GET INTERRUPTED BY MESSAGES UNTIL THEY CALL THEIR FIRST startNewRound()
//            this.electedLeader = null; //ALREADY HANDLED BY HeartbeatListener
//            this.timeoutCounted = false;
            this.roundNumber ++;
            LOGGER.info("LeaderCandidate" + getNodeId() + " participated to new round.");
            int nodeRank = (int)(1 + Math.random()*100);
            System.out.println("NEW ELECTION :: ROUND NUMBER IS " + roundNumber + " MY RANK IS " + nodeRank + " :: " +  java.time.LocalTime.now());
            dcf.writeACommand(roundNumber + ",if(!result.timeout){nodeRanks.push({client:\""+ getNodeId() + "\",rank:" +
                    nodeRank +"})};");
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

        LOGGER.info("LeaderCandidate of " + nodeId + " started");
        DistributedConsensus consensusFramework = DistributedConsensus.getDistributeConsensus(leaderCandidate);
        consensusFramework.startRound();
    }

    public static void main(String[] args){
        String id = UUID.randomUUID().toString();
        LeaderCandidate.electLeader(id, args[0], args[1]);
    }
}