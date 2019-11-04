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
            Thread.sleep(8000);
            System.out.println("LEADER FAILED :: " + java.time.LocalTime.now());
            LOGGER.info("Leader failed :: " + java.time.LocalTime.now());
            follower.startNewRound();
        } catch (InterruptedException e) {
//            if (follower.isVal()){
//                follower.setVal(false);
//                System.out.println("GOT A MESSAGE :: STARTING NEW ROUND");
//                follower.startNewRound();
//            }
//            else{
            if(follower.getElectedLeader() == null){
                follower.setElectedLeader("abc");
            }

            System.out.println("GOT HB :: " + java.time.LocalTime.now());
            run();
//            }

        }
    }

}
