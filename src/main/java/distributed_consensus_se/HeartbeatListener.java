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
            System.out.println("asked to wait : leader is" + follower.getElectedLeader() + " TIME :" + java.time.LocalTime.now() + " thread : " + Thread.currentThread().getId());
            Thread.sleep(8000);
            System.out.println(follower.getElectedLeader() + " leader failed. TIME : " + java.time.LocalTime.now());
            LOGGER.info("Leader failed.");
            follower.startNewRound();
        } catch (InterruptedException e) {
            System.out.println(follower.getElectedLeader() + " sent the HB : TIME :" + java.time.LocalTime.now());
            run();
        }
    }
}
