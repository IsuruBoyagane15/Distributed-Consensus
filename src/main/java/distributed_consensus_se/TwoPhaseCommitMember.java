package distributed_consensus_se;

import org.graalvm.polyglot.Value;

import java.util.Random;
import java.util.UUID;

public class TwoPhaseCommitMember extends ConsensusApplication {

    private  boolean voted;
    private boolean timeoutHandled;

    public TwoPhaseCommitMember(String nodeId, String runtimeJsCode, String evaluationJsCode, String kafkaServerAddress,
                                String kafkaTopic) {
        super(nodeId, runtimeJsCode, evaluationJsCode, kafkaServerAddress, kafkaTopic);
        this.timeoutHandled = false;
        this.voted = false;
    }

    @Override
    public void handleHeartbeat(String sender) {

    }

    @Override
    public boolean onReceiving(Value evaluationOutput) {
        DistributedConsensus dcf = DistributedConsensus.getDistributeConsensus(this);
        if (evaluationOutput.getMember("coordinatorElected").toString().equals(this.getNodeId())){
            if (!timeoutHandled){
                try {
                    System.out.println("I AM THE COORDINATOR");
                    Thread.sleep(8000);
                    System.out.println("TIME IS UP");
                    this.timeoutHandled = true;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                dcf.writeACommand("timeout = true;");
                return false;
            }
            else if(evaluationOutput.getMember("commitOrAbort").isNull()){
                if (evaluationOutput.getMember("votesResult").isBoolean()){
                    if (evaluationOutput.getMember("votesResult").asBoolean()) {
                        dcf.writeACommand("result.commitOrAbort = true;");
                    }
                    else if(!evaluationOutput.getMember("votesResult").asBoolean()){
                        dcf.writeACommand("result.commitOrAbort = false;");
                    }
                }
                return false;
            }
            else{
                System.out.println("FINISHING");
                dcf.writeACommand("participants = [];timeout = false;result = {coordinatorElected: null, votesResult:null, commitOrAbort:null};");
                return true;
            }
        }
        else{
            if (!voted){
                System.out.println("I AM A PARTICIPANT");

                Random random = new Random();
                boolean canCommit = random.nextBoolean();
                dcf.writeACommand("voteForCommit(\""+ getNodeId() + "\"," + canCommit + ");");
                voted = true;
                return false;
            }
            else{
                return !evaluationOutput.getMember("commitOrAbort").isNull();
            }
        }
    }

    @Override
    public void commitAgreedValue(Value evaluationOutput) {
        DistributedConsensus dcf = DistributedConsensus.getDistributeConsensus(this);
        dcf.setTerminate(true);
        if(evaluationOutput.getMember("commitOrAbort").asBoolean()){
            System.out.println("committed");
        }
        else{
            System.out.println("Did not commit");
        }
    }

    public static void twoPhaseCommit(String nodeId, String kafkaServerAddress, String kafkaTopic){
        TwoPhaseCommitMember twoPCParticipant = new TwoPhaseCommitMember(nodeId,
                "participants = [];timeout = false;result = {coordinatorElected: null, votesResult:null, commitOrAbort:null};" +
                "function voteForCommit( id, v ) {" +
                "   for (var i in participants) {" +
                "     if (participants[i].participant == id) {" +
                "        participants[i].vote = v;" +
                "        break;" +
                "     }" +
                "   }" +
                "};"
                ,

        "if(result.coordinatorElected != null){" +
                "    if(timeout == true){" +
                "        if (!participants.some(participant => participant.vote == null)){" +
                "            if(participants.every(participant  => participant.vote == true)){" +
                "                result.votesResult = true;" +
                "            }" +
                "            else{" +
                "                result.votesResult = false;" +
                "            }" +
                "        }   " +
                "    }" +
                "}" +
                "result" ,

                kafkaServerAddress, kafkaTopic);

        DistributedConsensus dcf = DistributedConsensus.getDistributeConsensus(twoPCParticipant);
        dcf.start();
        dcf.writeACommand(
                "if (result.coordinatorElected == null){" +
                "    result.coordinatorElected = \"" + twoPCParticipant.getNodeId() +"\";" +
                "}" +
                "else if (timeout == false){" +
                    "participants.push({participant:\""+ twoPCParticipant.getNodeId() + "\",vote: null});" +
                "}");
    }
    public static void main(String args[]){
        String Id = UUID.randomUUID().toString();
        TwoPhaseCommitMember.twoPhaseCommit(Id, args[0], args[1]);
    }


}
