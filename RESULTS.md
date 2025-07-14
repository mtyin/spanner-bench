# Instance setup

- Multi-region with nam15
- 1 node
- no autoscaler
- Client machine: n4-standard-16 (16 vCPUs, 64 GB Memory) @ us-south1-a
- NOTE: all results below are based on DML (instead of Mutation API). Mutation
  API should give you better results.

# Graph setup

- ~1M nodes
- ~12M edges
- average fan-in: 12, average fan-out: 12

# Read-modify-write(edge)

- Parameter:
    - Concurrency: 30, NumOperations 1M, insertOrUpdateEdgeUpdCount
- Utilization: 60% utilization at leader region
- Throughput: 526 op per second
- Latency: 66ms @ 99th, 48ms @ 50th
- Screenshot: screen/38HtRnB3E3TCBTx

# Read-insert(edge)

- Parameter:
    - Concurrency: 20, NumOperations 1M, findOrCreateEdge with random ids,
      hence almost all read-miss-create.
    - Edge has two indexes (FK and reverse edge index)
- Utilization: 50% utilization at leader region
- Throughput: 353 op per second
- Latency: 62ms @ 99th, 44ms @ 50th
- Screenshot: screen/4PeEm4b2GQS9LqY

# Read-insert-2(edge)

- Parameter:
    - Concurrency: 20, NumOperations 1M, findOrCreateEdge with random ids,
      hence almost all read-miss-create.
    - Edge has only one indexes (FK is informational and one reverse edge index)
- Utilization: 40% utilization at leader region
- Throughput: 448 op per second
- Latency: 53ms @ 99th, 39ms @ 50th
- Screenshot: screen/94SfrWtXkwKWMQb

# Query-1

- Parameter:
    - Concurrency: 15, NumOperations 1M, findSubgraph
    - Client @ us-south1-a
- Utilization: 45% utilization at us-south1 (close to 0 in the other region)
- Throughput: 2.9k op per second
- Latency: 4.4ms @ 99th, 2.6ms @ 50th
- Screenshot: screen/BoaAZorJcy7Jtrp
- FindSubgraph returns 26 rows on average (fan-in + fan-out): screen/BLidWUH8CGci5Fr

# Query-2

- Parameter:
    - Concurrency: 75, NumOperations 1M, findSubgraph
    - Client @ us-east4-a
- Utilization: 50% utilization at us-east4 (close to 0 in the leader region)
- Throughput: 1.85k op per second
- Latency: 57ms @ 99th, 40ms @ 50th
- Screenshot: screen/cthvJiZgXMWWhW3
- FindSubgraph returns 26 rows on average (fan-in + fan-out): screen/sNF4aP39s5E2VZb
