# Distribute-Consensus-Genaralized
Distributed consensus algorithms and an API supports to implement algorithms using Kafka messaging. 

Based on the nature of distributed algorithm (round based/non-round based) developer can use customized API of DistributedConsensus.

In round based algorithms a round is an independent algorithm execution.
In a round, consensus related to that algorithm is achieved.
The separation of rounds in the Kafka is done using round numbers.
kafka records with same round number are the events happened in that round.
Leader election algorithm implemented here is an round based algorithm.

In non-round based algorithms the complete kafka log contains events of same algorithm execution.
Therefore no need of separating rounds in the Kafka log (It can be considered as a single round).
Far a non-round based solution Distributed Lock algorithm is implemented. 
