package distributedConsensus;

import org.graalvm.polyglot.Value;

/**
 * Abstract class of Java applications which use Kafka messaging and Javascript evaluation
 * to perform various consensus use cases
 */
public abstract class ConsensusApplication {
    protected final DistributedConsensus distributedConsensus;
    protected String nodeId, runtimeJsCode, evaluationJsCode, kafkaTopic, kafkaServerAddress;

    /**
     * Constructor
     *
     * @param nodeId unique id to identify the LockHandler
     * @param runtimeJsCode String containing Javascript records
     * @param evaluationJsCode Javascript logic to evaluate and achieve consensus
     * @param kafkaServerAddress URL of Kafka server
     * @param kafkaTopic Kafka topic to subscribe to participate when achieving distributed consensus
     */
    public ConsensusApplication(String nodeId, String runtimeJsCode, String evaluationJsCode,
                                String kafkaServerAddress, String kafkaTopic){
        this.nodeId = nodeId;
        this.runtimeJsCode = runtimeJsCode;
        this.evaluationJsCode = evaluationJsCode;
        this.kafkaTopic = kafkaTopic;
        this.kafkaServerAddress = kafkaServerAddress;
        this.distributedConsensus = new DistributedConsensus(this);
    }

    /**
     * Extract whether consensus achieved or not from Javascript result
     *
     * @param result Javascript evaluation result
     * @return whether consensus is achieved or not
     */
    public abstract boolean checkConsensus(Value result);

    /**
     * Action taken after achieving consensus
     *
     * @param evaluationOutput Javascript evaluation result
     */
    public abstract void onConsensus(Value evaluationOutput);

    /**
     * Get KafkaTopic
     *
     * @return KafkaTopic
     */
    public String getKafkaTopic() {
        return kafkaTopic;
    }

    /**
     * Get NodeId
     *
     * @return NodeId
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * Get runtimeJsCode
     *
     * @return runtimeJsCode
     */
    public String getRuntimeJsCode() {
        return runtimeJsCode;
    }

    /**
     * Set runtimeJsCode to runtimeJsCode
     *
     * @param runtimeJsCode set
     */
    public void setRuntimeJsCode(String runtimeJsCode) {
        this.runtimeJsCode = runtimeJsCode;
    }

    /**
     * Get evaluationJsCode
     *
     * @return evaluationJsCode
     */
    public String getEvaluationJsCode() {
        return evaluationJsCode;
    }

    /**
     * Get kafkaServerAddress
     *
     * @return kafkaServerSAddress
     */
    public String getKafkaServerAddress() {
        return kafkaServerAddress;
    }
}
