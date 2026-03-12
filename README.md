# DistributedDB

### How to Run

1. Ensure the /data/ folder in /data-node/ is removed entirely to avoid old state from restoring on fresh start
2. `./gradlew clean build`

Ideally run each entity [client, data node, seed node] from separate terminals to follow through execution.  
The data nodes and clients will sit idle until the seed node freezes the partition map.

- To freeze, the seed node must pass two conditions
  1. There are at least n data nodes registered where n == replication factor
  2. There has been no changes to the cluster membership (new node joining, registered node leaving, etc) for some amount of time (default set to 10s of no change)

3. Start seed node  
   `./gradlew :seed-node:run`
4. Start n data nodes with unique port numbers  
   `./gradlew :data-node:run --args=<port_number>`
5. Start client(s)  
   `./gradlew :client:run`
6. Execute commands, kill data nodes, etc and see outputs to test the system

### Division of Work

I was alone so I did everything (please take that into consideration). I hate Raft...
