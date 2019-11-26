# distribute-consensus
Distributed consensus algorithms using Kafka messaging. 

There are two consensus algorithms implemented here.

##Leader Election
Leader election algorithm implemented here is an round based algorithm.
A leader election round(round number = n) is a set of state changes written to Kafka to elect one leader.
After that leader died a new leader election round(round number = n+1) is started.
The separation of rounds in the Kafka log is done using these integer round numbers.
Kafka records with same round number are the events happened in that round.

Tester is implemented to start/kill new leader candidates(threads) randomly. 
Tester will start/kill leader candidate threads until a given maximum number of leader candidate threads are created.
After achieving the maximum number Tester will only kill leader candidate threads.
When the number of active leader candidate threads is zero test run is finished.

###Usage

1. Build Tester using *mvn clean install*

1. Run [Zookeeper](https://zookeeper.apache.org/doc/r3.1.2/zookeeperStarted.html). 
2. Run [Kafka](https://kafka.apache.org/quickstart).

    * (You can follow [this](https://kafka.apache.org/quickstart#quickstart_startserver)  Kafka quick start to start a single node Zookeeper and Kafka services)

4. Run the Jar  giving location to save the log, Kafka server address, kafka topic and maximum number of leader candidate threads to run.

    __java -Dpath=<_locatoin to save the log file_> -jar <_location to the jar built in 3._> <_kafka server address_> <_kafka topic_> <_maximum leader candidate thread count_>__
    
    ex: _java -Dpath=/home/JohnDoe/test0.log -jar Tester.jar localhost:9092 election 50_

##Distribute Lock
Distributed Lock algorithm implemented here has no rounds. 
In distributed lock algorithm the complete kafka log contains events of same algorithm execution.
Therefore no need of separating rounds in the Kafka log (Here, Kafka entire log can be considered as a single round).
