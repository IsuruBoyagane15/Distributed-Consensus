package distributedConsensus;

import org.apache.log4j.Logger;

public class HeartbeatListener extends Thread {
    private LeaderCandidate follower;

    private static final Logger LOGGER = Logger.getLogger(LeaderCandidate.class);

    public HeartbeatListener(LeaderCandidate follower){
        this.follower = follower;
    }

    public void run() {
        try {
            Thread.sleep(2000);
            System.out.println("LEADER FAILED :: " + java.time.LocalTime.now());
            follower.setElectedLeader(null);
            LOGGER.info("Identified leader FAILURE");

            follower.startNewRound();
        } catch (InterruptedException e) {
            run();
        }
    }
}
