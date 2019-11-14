package distributedConsensus;

import org.apache.log4j.Logger;

public class HeartbeatListener extends Thread {
    private LeaderCandidate follower;

    private static final Logger LOGGER = Logger.getLogger(LeaderCandidate.class);

    public HeartbeatListener(LeaderCandidate follower){
        this.follower = follower;
    }

    public void run() {
        int i = 0;
        while(i<=200){
            if (i == 200){
                LOGGER.info("Identified leader FAILURE; Listener thread should finish here");
                follower.setElectedLeader(null);
                follower.startNewRound();
                break;
            }
            else{
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    if (follower.isLateToTimeout()){
                        follower.setLateToTimeout(false);
                        LOGGER.info("Late to timeout; Listener thread should finish here");
                        break;
                    }
                    else{
                        i = 0;
                    }
                }
            }
            i++;
        }
    }
}
