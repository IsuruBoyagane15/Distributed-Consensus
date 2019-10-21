package distributed_consensus_se;

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


    public abstract boolean onReceiving(Value evaluationOutput);

    public abstract void commitAgreedValue(Value evaluationOutput);

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

    public void setEvaluationJsCode(String evaluationJsCode) {
        this.evaluationJsCode = evaluationJsCode;
    }

    public String getKafkaServerAddress() {
        return kafkaServerAddress;
    }

//    public abstract boolean participate(DistributedConsensus.roundStatuses nextRoundStatus, int nextRoundNumber, String nextRoundCode);
}
