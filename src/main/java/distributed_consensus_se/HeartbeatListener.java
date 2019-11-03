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
            System.out.println("LEADER FAILED");
            LOGGER.info("Leader failed :: " + java.time.LocalTime.now());
            follower.startNewRound();
        } catch (InterruptedException e) {
            if(follower.getElectedLeader() == null){
                follower.setElectedLeader("abc");
            }
            System.out.println("GOT HB :: " + java.time.LocalTime.now());
//            if(follower.isPending()){
//                follower.setPending(false);
//            }
            run();
        }
    }

}
