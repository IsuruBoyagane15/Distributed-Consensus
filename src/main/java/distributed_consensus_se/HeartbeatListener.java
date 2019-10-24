package distributed_consensus_se;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeartbeatListener extends Thread {
    private LeaderCandidate follower;

    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderCandidate.class);

    public HeartbeatListener(LeaderCandidate follower){
        this.follower = follower;
    }

    public void run() {
        try {
            Thread.sleep(4000);
            LOGGER.info("Leader failed.");
            follower.startNewRound();
        } catch (InterruptedException e) {
            run();
        }
    }

    public void start(){

    }
}
