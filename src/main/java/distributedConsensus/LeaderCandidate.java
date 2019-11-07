package distributedConsensus;

import org.graalvm.polyglot.Value;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

public class LeaderCandidate extends ConsensusApplication{

    static{
        String id = UUID.randomUUID().toString();
        System.setProperty("id", id);
    }
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
            LOGGER.info("Participated to round :" + roundNumber + " rank :" + nodeRank );

        }

        else if(myState == DistributedConsensus.roundStatuses.ONGOING){
            System.out.println("PARTICIPATED :: MY RANK IS " + nodeRank + " :: "  + java.time.LocalTime.now());
            setRuntimeJsCode(initialJsCode + myRoundCodes);
            consensusFramework.writeACommand(proposedRoundNumber + ",if(!result.timeout){nodeRanks.push({client:\""+ getNodeId() + "\",rank:" +
                    nodeRank +"});}");
            LOGGER.info("Participated to round :" + roundNumber + " rank :" + nodeRank );

        }

        else if(myState == DistributedConsensus.roundStatuses.FINISHED){
            LOGGER.info("Waiting for HB or Start any message to follow up and catch a leader failure" );
            System.out.println("WAITING FOR HBs :: WILL JOIN TO NEXT ROUND" + " :: " +  java.time.LocalTime.now());
            startHeartbeatListener();
        }
        else{
            LOGGER.error("WRONG STATE found for last round");
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
                    LOGGER.info("Round number is increment for participate round " + roundNumber);
                    skipARound = true;

                }
                System.out.println("SOMEONE HAS STARTED NEW ROUND");
                LOGGER.info("Could not participate to round. Skipping the round");
                listeningThread.interrupt();
                return false;
            }

//            System.out.println("FIRST MEMBER IS " + result.getMember("firstCandidate").toString());
            if (result.getMember("firstCandidate").toString().equals(getNodeId()) && !timeoutCounted){
                long timeout = 500;
                try {
                    Thread.sleep(timeout);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                this.timeoutCounted = true;
                dcf.writeACommand(this.roundNumber + ",result.timeout = true;");
                LOGGER.info("Waited " + timeout + " wrote result.timeout = true; to close the vote counting");
                System.out.println("WROTE TIMEOUT " + ":: " +  java.time.LocalTime.now());
                return false;

            }
            else{
                LOGGER.info("Leader is not yet decided");
                return checkConsensus(result);
            }
        }
        else{
            LOGGER.error("onReceive() when LEADER is a leader.");
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
        this.listeningThread.interrupt();
    }

    @Override
    public boolean checkConsensus(Value result) {
        return result.getMember("consensus").asBoolean();
    }

    public void startHeartbeatSender(){
        LOGGER.info("Started sending HB");
        while (true) {
            System.out.println("ALIVE " + ":: " +  java.time.LocalTime.now());
            DistributedConsensus consensusFramework = DistributedConsensus.getDistributeConsensus(this);
            consensusFramework.writeACommand(roundNumber + ",ALIVE,"+ getNodeId());
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                LOGGER.error("Leader was interrupted while sending HB");
                e.printStackTrace();
            }
        }
    }

    public void startHeartbeatListener(){
        LOGGER.info("Started listening to HB");
        this.listeningThread = new Thread(new HeartbeatListener(this));
        this.listeningThread.start();
    }

    public void cleanRound(int roundNumber){
        this.setRuntimeJsCode(initialJsCode); // IN EACH NEW ROUND JS SHOULD BE RESET
        this.joiningState = null; //SHOULD BE DONE SINCE "FINISHED" NODES GET INTERRUPTED BY MESSAGES UNTIL THEY CALL THEIR FIRST startNewRound()
        this.timeoutCounted = false;
        this.electedLeader = null;
        LOGGER.info("Cleaned round data of round number " + roundNumber);
    }

    public void startNewRound(){
            DistributedConsensus dcf = DistributedConsensus.getDistributeConsensus(this);
//            this.setRuntimeJsCode(initialJsCode); // DONE IN CONSUMER LOOP WHEN EVALUATING SECTION FINDS A HIGHER ROUNDNUMBERED RECORD
//            this.joiningState = null; //SHOULD BE DONE SINCE "FINISHED" NODES GET INTERRUPTED BY MESSAGES UNTIL THEY CALL THEIR FIRST startNewRound()
//            this.electedLeader = null; //ALREADY HANDLED BY HeartbeatListener
//            this.timeoutCounted = false;
            this.roundNumber ++;
            LOGGER.info("Participated to new round.");
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

        String node_id = System.getProperty("id");
        LeaderCandidate.electLeader(node_id, args[0], args[1]);
    }
}