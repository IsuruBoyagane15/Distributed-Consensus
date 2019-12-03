package leaderElection;

import org.apache.log4j.Logger;


/**
 * Class to listen to heartbeats of a elected leader and identify void of heartbeats when a leader
 * is failed and call a new leader
 */
public class HeartbeatListener extends Thread {
    private LeaderCandidate follower;
    private boolean lateToTimeout;
    private static final Logger LOGGER = Logger.getLogger(LeaderCandidate.class);

    /**
     * Constructor
     *
     * @param follower LeaderCandidate which become a follower or joined to a FINISHED round
     */
    public HeartbeatListener(LeaderCandidate follower){
        this.follower = follower;
        this.lateToTimeout = false;
    }

    public void setLateToTimeout(boolean lateToTimeout) {
        this.lateToTimeout = lateToTimeout;
    }

    /**
     * Run method of HeartbeatListener
     */
    public void run() {
        int i = 0;
        while(i<=200 && !follower.isTerminate()){
            if (i == 200){
                LOGGER.info("Identified leader FAILURE");
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
                        LOGGER.info("Got a higher round number(N) Kafka record, Late to timeout, " +
                                "will evaluate records of round N without writing the vote");
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
