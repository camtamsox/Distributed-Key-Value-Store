This is a class project and contains stencil code. I implemented a single-server, concurrent store and also a sharded, distributed system capable of scaling across multiple servers.

## Concurrent Key-Value Store

Supports Get, Put, Append, Delete, MultiGet, MultiPut, and AllKeys operations.

Utilizes a fine-grained locking strategy with a bucket-based hashtable where each bucket has its own readers-writer lock. This design allows multiple threads to operate on different buckets simultaneously while being thread safe.

GDPR-compliant deletion mechanism that selectively removes user content based on keywords.

## Distributed Key-Value Store

Scaled system by sharding the data across multiple servers (partitions are based on first character of the key).

The shardcontroller manages the distribution of shards across servers. Supports the operations join, leave, move, and query.

The sharding-aware servers automatically join/leave the shard controller, check for configuration changes, and transfer data when they are no longer responsible for a shard.

The sharding-aware client interacts with the sharded system by routing the given requests.

## How to Use

To compile, you must go to the build directory and run "make -j"

To run the concurrent store:

1. Start the server using "./server 5000" in one terminal (or whatever port you want)
2. Start the client using "./simple_client localhost:5000" in another terminal

To run the distributed store:

1. Start the shardcontroller in a terminal with "./shardcontroller 1234"
2. Start the servers in seperate terminals with the address printed by the shardcontroller

"./server 1234 29f0c19d87b:1234 2" (usage: ./server <port> <shardcontroller_address> <num_workers>)

3. Connect the Sharding-Aware Client with "./client 29f0c19d87b:1234" (usage: ./client <shardcontroller_address>)
