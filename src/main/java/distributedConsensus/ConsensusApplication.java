package distributedConsensus;

import org.graalvm.polyglot.Value;

public abstract class ConsensusApplication {
    private String nodeId, runtimeJsCode, evaluationJsCode, kafkaTopic, kafkaServerAddress;

    public ConsensusApplication(String nodeId, String runtimeJsCode, String evaluationJsCode, String kafkaServerAddress,
                                String kafkaTopic){
        this.nodeId = nodeId;
        this.runtimeJsCode = runtimeJsCode;
        this.evaluationJsCode = evaluationJsCode;
        this.kafkaTopic = kafkaTopic;
        this.kafkaServerAddress = kafkaServerAddress;
    }


    public abstract void handleHeartbeat();

    public abstract boolean checkConsensus(Value result);

    public abstract boolean onReceiving(Value evaluationOutput);

    public abstract void onConsensus(Value evaluationOutput);

    public abstract void participate(DistributedConsensus.roundStatuses myState, int myRoundNumber, String myRoundCodes);

    public abstract void cleanRound();

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getRuntimeJsCode() {
        return runtimeJsCode;
    }

    public void setRuntimeJsCode(String runtimeJsCode) {
        this.runtimeJsCode = runtimeJsCode;
    }

    public String getEvaluationJsCode() {
        return evaluationJsCode;
    }

    public String getKafkaServerAddress() {
        return kafkaServerAddress;
    }

}
