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
            System.out.println("WILL ELECT NEW LEADER OVER " + follower.getElectedLeader() + " IN 8S. : TIME :" + java.time.LocalTime.now() + " thread : " + Thread.currentThread().getName() + "\n");
            Thread.sleep(8000);
            System.out.println(follower.getElectedLeader() + " FAILED. TIME : " + java.time.LocalTime.now());
            LOGGER.info("Leader failed.");
            follower.startNewRound();
        } catch (InterruptedException e) {
            run();
        }
    }
}
