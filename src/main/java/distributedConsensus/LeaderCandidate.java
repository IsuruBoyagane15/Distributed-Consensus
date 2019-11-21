package distributedConsensus;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.graalvm.polyglot.Value;
import org.apache.log4j.Logger;

public class LeaderCandidate extends ConsensusApplication implements Runnable{

    enum roundStatuses {
        ONGOING,
        NEW,
        FINISHED
    }
    private static final Logger LOGGER = Logger.getLogger(LeaderCandidate.class);
    private roundStatuses joiningState;
    private boolean timeoutCounted, terminate;
    private int roundNumber;
    private HeartbeatListener heartbeatListener;
    private String electedLeader;
    private final String initialJsCode;

    public LeaderCandidate(String nodeId, String runtimeJsCode, String evaluationJsCode, String kafkaServerAddress,
                           String kafkaTopic) {
        super(nodeId, runtimeJsCode, evaluationJsCode, kafkaServerAddress, kafkaTopic);
        this.initialJsCode = runtimeJsCode;
        this.heartbeatListener = null;
        this.electedLeader = null;
        this.timeoutCounted = false; //FIRST LEADER CANDIDATE TO JOIN THAT ROUND WILL COUNT A TIMEOUT AND CLOSE THE VOTING BY WRITING IT TO KAFKA
        this.joiningState = null; //STATE OF THE ROUND WHEN NODE PARTICIPATED;
        this.terminate = false;
    }

    public boolean isTerminate() {
        return terminate;
    }

    public void setTerminate(boolean terminate) {
        this.terminate = terminate;
    }

    public void setElectedLeader(String electedLeader) {
        this.electedLeader = electedLeader;
    }

    public void participate(int lastRoundNumber, String lastRoundJsCodes) {
        DistributedConsensus consensusFramework = DistributedConsensus.getDistributeConsensus(this);
        int nodeRank = (int)(1 + Math.random()*100);
        this.roundNumber = lastRoundNumber;
        this.setRuntimeJsCode(initialJsCode);

        if (lastRoundJsCodes.equals("")){
            //EMPTY KAFKA LOG
            this.joiningState = roundStatuses.NEW;
            consensusFramework.writeACommand(this.roundNumber+ ",if(!result.timeout){nodeRanks.push({client:\""+ getNodeId() + "\",rank:" +
                    nodeRank +"});}");
            LOGGER.info("Participated to NEW round :" + roundNumber + "; rank is " + nodeRank  );
        }
        else{
            //NON-EMPTY KAFKA LOG
            Value latestRoundResult = consensusFramework.evaluateJsCode(lastRoundJsCodes);
            boolean isRoundFinished = this.checkConsensus(latestRoundResult);
            if (isRoundFinished){
                //NON-EMPTY KAFKA LOG WITH FINISHED ROUND
                this.joiningState = roundStatuses.FINISHED;
                LOGGER.info("Waiting for HBs of FINISHED round " + roundNumber + "; Or will join to round " + (roundNumber + 1));
                startHeartbeatListener();
            }
            else{
                //NON-EMPTY KAFKA LOG WITH ONGOING ROUND
                this.joiningState = roundStatuses.ONGOING;
                setRuntimeJsCode(initialJsCode + lastRoundJsCodes);
                consensusFramework.writeACommand(this.roundNumber + ",if(!result.timeout){nodeRanks.push({client:\""+ getNodeId() + "\",rank:" +
                        nodeRank +"});}");
                LOGGER.info("Participated to ONGOING round :" + roundNumber + "JsCode : " + lastRoundJsCodes + "; rank is " + nodeRank);
            }
        }
    }

    //SHOULD NOT BE CALLED WHEN THERE IS AN IDENTIFIED LEADER
    public boolean onReceiving(Value result) {
        DistributedConsensus dcf = DistributedConsensus.getDistributeConsensus(this);
        if(electedLeader == null){
            if (result.getMember("firstCandidate").toString().equals(getNodeId()) && !timeoutCounted){
                long timeout = 500;
                try {
                    Thread.sleep(timeout);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                this.timeoutCounted = true;
                dcf.writeACommand(this.roundNumber + ",result.timeout = true;");
                LOGGER.info("Waited " + timeout + "ms and wrote \"result.timeout = true;\" to close the vote counting");
                return false;
            }
            else{
                return checkConsensus(result);
            }
        }
        else{
            LOGGER.info("Record of same round " + roundNumber + "after leader is elected");
            return false;
        }
    }

    @Override
    public void onConsensus(Value value) {
        this.electedLeader = value.getMember("value").toString();
        LOGGER.info(getNodeId() + " :: " + this.electedLeader + " is elected as the leader");
        if (value.getMember("value").toString().equals(getNodeId())) {
            this.startHeartbeatSender();
        }
        else{
            this.startHeartbeatListener();
        }
    }

    public void handleHeartbeat() {
        this.heartbeatListener.interrupt();
    }

    @Override
    public boolean checkConsensus(Value result) {
        return result.getMember("consensus").asBoolean();
    }

    public void start(){

        DistributedConsensus distributedConsensus = DistributedConsensus.getDistributeConsensus(this);
        final String rawString = distributedConsensus.generateUniqueKey();
        final String unique_round_key = DigestUtils.sha256Hex(rawString);
        final String checkRecord = "CHECK,"+ unique_round_key;

        distributedConsensus.writeACommand(checkRecord);
        LOGGER.info("Started; Id : " + getNodeId() + "; " + "check message : " + checkRecord);

        Runnable consuming = () -> {
            boolean correctRoundIdentified = false;

            String latestRoundsJsCode = "";
            int latestRoundNumber = 0;

            try {
                while (!this.terminate) {
                    ConsumerRecords<String, String> records = distributedConsensus.getMessages();
                    for (ConsumerRecord<String, String> record : records) {
                        if (!correctRoundIdentified){
                            //IDENTIFYING THE ROUND
                            if (record.value().equals(checkRecord)) {
                                //TAKE DECISION ON ROUND STATUS BASED ON COLLECTED LAST ROUND CODES AND PARTICIPATE

                                LOGGER.info("Found check record : " + checkRecord);
                                this.participate(latestRoundNumber,latestRoundsJsCode);
                                correctRoundIdentified = true;

                            }
                            else if (!record.value().startsWith("CHECK,")){ //A NODE ONLY CONSIDER IT'S CHECK RECORD
                                //COLLECT MOST RECENT ROUND'S CODE
                                String[] recordContent = record.value().split(",", 2);
                                int recordRoundNumber = Integer.parseInt(recordContent[0]);
                                String recordMessage = recordContent[1]; //ALIVE or clean JS

                                if (!recordMessage.startsWith("ALIVE")){
                                    if (recordRoundNumber>latestRoundNumber){
                                        //THERE IS A NEW ROUND IN KAFKA
                                        LOGGER.info("Discard " + latestRoundNumber +", since there is a new round in kafka log before the check record");
                                        latestRoundsJsCode = recordMessage;
                                        latestRoundNumber = recordRoundNumber;
                                    }
                                    else if(recordRoundNumber==latestRoundNumber){
                                        latestRoundsJsCode += recordMessage;
                                    }
                                    //RECORDS WITH ROUND NUMBERS LESS THAN latestRoundNumber CANNOT BE FOUND
                                }
                                //ALIVE RECORDS ARE NOT ADDED TO THE LATEST ROUND CODE
                            }
                        }
                        else if (!record.value().startsWith("CHECK,")){
                            //EVALUATING RECORDS OF ROUND TO WHICH NODE PARTICIPATED
                            String[] recordContent = record.value().split(",", 2);
                            int recordRoundNumber = Integer.parseInt(recordContent[0]); //Round number written with the record
                            String recordMessage = recordContent[1]; //ALIVE,nodeId or clean JS

                            if (this.joiningState == roundStatuses.FINISHED){
                                // NEWLY JOINED NODES WITH FINISHED STATE FIRST EXECUTE THIS
                                if (recordRoundNumber == this.roundNumber){
                                    //RECORDS (HBs) WITH ROUND NUMBER AS proposedRoundNumber
                                    LOGGER.info("Got HB of FINISHED round");
                                    this.handleHeartbeat();
                                }
                                else if(recordRoundNumber == this.roundNumber + 1){
                                    //SOMEONE HAS TIMEOUT BEFORE THIS NODE
                                    LOGGER.info("Got new round message while in FINISHED state");
                                    this.heartbeatListener.setLateToTimeout(true);
                                    if (this.heartbeatListener.isAlive()){
                                        //TERMINATE LISTENER STARTED FOR FINISHED, TO MOVE TO NEW ROUND
                                        LOGGER.info("Late to timeout the round " + this.roundNumber);
                                        this.heartbeatListener.interrupt();
                                    }
                                    //WAIT UNTIL LISTENER IS FINISHED
                                    this.heartbeatListener.join();
                                    //CLEAN UPON THE FIRST (roundNumber + 1) RECORD
                                    this.cleanRound(recordRoundNumber);
                                    Value result = distributedConsensus.evaluateJsCode(recordMessage);
                                    boolean consensusAchieved = this.onReceiving(result);
                                    if (consensusAchieved) {
                                        this.onConsensus(result);
                                    }
                                    else{
                                        LOGGER.info("Leader for " + this.roundNumber +  " is not elected yet");
                                    }
                                }
                                else{
                                    LOGGER.error("Record with wrong round number while in FINISHED state");
                                }
                            }
                            else{
                                //NON-FINISHED STATE NODES IN ANY ROUND
                                if (recordRoundNumber == roundNumber + 1){
                                    //CLEAN ALL ROUND RELATED DATA IN CONSENSUS APPLICATION WHEN THE FIRST MESSAGE TO LATEST ROUND COMES
                                    this.heartbeatListener.join();
                                    this.cleanRound(recordRoundNumber); //SETS THE ROUND NUMBER TO MEW RECORDS ROUND NUMBERS
                                }
                                if(recordMessage.startsWith("ALIVE")){
                                    if (this.roundNumber == recordRoundNumber){
                                        this.handleHeartbeat();
                                        LOGGER.info("Got HB");
                                    }
                                    else{
                                        LOGGER.error(getNodeId() + " :: Error: ALIVE with wrong round number");
                                        System.exit(-1);
                                    }
                                }
                                else{
                                    if(this.roundNumber == recordRoundNumber){
                                        LOGGER.info("Evaluating records of current round with round number : " + recordRoundNumber);
                                        Value result = distributedConsensus.evaluateJsCode(recordMessage);
                                        boolean consensusAchieved = this.onReceiving(result);
                                        if (consensusAchieved) {
                                            this.onConsensus(result);
                                        }
                                        else{
                                            LOGGER.info("Leader for " + this.roundNumber +  " is not elected yet");
                                        }
                                    }
                                    else{
                                        LOGGER.error("Error: Js record with wrong round number");
                                        System.exit(-1);
                                    }
                                }
                            }
                        }
                    }
                }
            } catch(Exception exception) {
                LOGGER.error("Exception occurred :", exception);
            }finally {
                distributedConsensus.closeConsumer();
            }
        };
        Thread consumer = new Thread(consuming);
        consumer.setName(getNodeId() + "_consumer");
        consumer.start();
    }

    public void startHeartbeatSender(){
        LOGGER.info("Started sending HB");
        while (!this.terminate) {
            DistributedConsensus consensusFramework = DistributedConsensus.getDistributeConsensus(this);
            consensusFramework.writeACommand(roundNumber + ",ALIVE,"+ getNodeId());
            LOGGER.info("wrote HB");
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                LOGGER.error("Leader was interrupted while sending HB :: " + java.time.LocalTime.now());
                e.printStackTrace();
            }
        }
    }

    public void startHeartbeatListener(){
        LOGGER.info("Started HB listener");
        this.heartbeatListener = new HeartbeatListener(this);
        this.heartbeatListener.setName(getNodeId() + "_HBListener");
        this.heartbeatListener.start();
    }

    public void cleanRound(int roundNumber){
        this.roundNumber  = roundNumber; //SET THE ROUND NUMBER TO NEW RECORD ROUND NUMBER
        this.setRuntimeJsCode(initialJsCode); // IN EACH NEW ROUND JS SHOULD BE RESET
        this.joiningState = null; //SHOULD BE DONE SINCE "FINISHED" NODES GET INTERRUPTED BY MESSAGES UNTIL THEY CALL THEIR FIRST startNewRound()
        this.timeoutCounted = false;
        this.electedLeader = null;
        this.heartbeatListener.setLateToTimeout(false);
        LOGGER.info("Cleaned round attributes of round number " + (roundNumber -1));
    }

    public void startNewRound(){
        DistributedConsensus dcf = DistributedConsensus.getDistributeConsensus(this);
        int nodeRank = (int)(1 + Math.random()*100);
        dcf.writeACommand((roundNumber+1) + ",if(!result.timeout){nodeRanks.push({client:\""+ getNodeId() + "\",rank:" +
                nodeRank +"})};");
        LOGGER.info("Participated to new round "+ (roundNumber + 1) + "; my rank is " + nodeRank);
    }

    public void run(){
        this.start();
    }
}