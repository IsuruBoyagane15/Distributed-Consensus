package distributed_consensus_se;

import org.graalvm.polyglot.Value;

import java.util.UUID;

public class TwoPhaseCommitMember extends ConsensusApplication {

    public TwoPhaseCommitMember(String nodeId, String runtimeJsCode, String evaluationJsCode, String kafkaServerAddress,
                            String kafkaTopic) {
        super(nodeId, runtimeJsCode, evaluationJsCode, kafkaServerAddress, kafkaTopic);
    }

    @Override
    public void handleHeartbeat() {

    }

    @Override
    public boolean onReceiving(Value evaluationOutput) {
        return !evaluationOutput.getMember("coordinatorElected").isNull();
    }

    @Override
    public void onConsensus(Value evaluationOutput) {
        if(evaluationOutput.getMember("coordinatorElected").toString().equals(getNodeId())){
            System.out.println("I am the cordinator");
        }
        else{
            System.out.println("Not the cordinator");

        }
    }

    @Override
    public void participate(DistributedConsensus.roundStatuses myState, int myRoundNumber, String myRoundCodes) {

    }

    public static void twoPhaseCommit(String nodeId, String kafkaServerAddress, String kafkaTopic){
        TwoPhaseCommitMember twoPCParticipant = new TwoPhaseCommitMember(nodeId,                 "participants = [];result = {coordinatorElected: null, coordinatorRequested:null, votesResult:null, commitOrAbort:null};",
                    "result" ,

//                "if (participantResponses.some(response => response.node == \"" + coordinatorId + "\" && response.vote === true)){" +
//                        "result.coordinatorRequested = true;" +
//                        "if(participantResponses.length == " + instanceCount + "){" +
//                        "if(participantResponses.every(response  => response.vote == true)){" +
//                        "result.votesResult = true;" +
//                        "}" +
//                        "else{" +
//                        "result.votesResult = false;" +
//                        "}" +
//                        "}" +
//                        "}" +
//                        "result",
                kafkaServerAddress, kafkaTopic);

        DistributedConsensus dcf = DistributedConsensus.getDistributeConsensus(twoPCParticipant);
        dcf.start();
        dcf.writeACommand("if (result.coordinatorElected == null){" +
                "    result.coordinatorElected = \"" + twoPCParticipant.getNodeId() +"\";" +
                "}" +
                "else{" +
                "participants.push({participant:\""+ twoPCParticipant.getNodeId() + "\",vote: null});" +
                "}");
    }
    public static void main(String args[]){
        String Id = UUID.randomUUID().toString();
        TwoPhaseCommitMember.twoPhaseCommit(Id, args[0], args[1]);
    }


}
