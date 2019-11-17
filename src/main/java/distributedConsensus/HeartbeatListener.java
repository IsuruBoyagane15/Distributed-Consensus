package distributedConsensus;

import org.apache.log4j.Logger;

public class HeartbeatListener extends Thread {
    private LeaderCandidate follower;
    private boolean lateToTimeout;

    private static final Logger LOGGER = Logger.getLogger(LeaderCandidate.class);

    public HeartbeatListener(LeaderCandidate follower){
        this.follower = follower;
        this.lateToTimeout = false;
    }

    public void setLateToTimeout(boolean lateToTimeout) {
        this.lateToTimeout = lateToTimeout;
    }

    public void run() {
        int i = 0;
        while(i<=200){
            if (i == 200){
                LOGGER.info(follower.getNodeId() + " Identified leader FAILURE");
                follower.setElectedLeader(null);
                follower.startNewRound();
                break;
            }
            else{
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    if (lateToTimeout){
                        lateToTimeout = false;
                        LOGGER.info("Late to timeout;");
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
