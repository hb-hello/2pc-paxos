# Fault-Tolerant Sharded Key Value Store with 2-phase commit
____________

Distributed banking system on top of a key values store implemented with -
- Two-phase commit protocol for coordination across shards
- Multi-Paxos for consensus within each shard

**Additional features:** Reconfiguration of cluster configuration, automatic resharding of data across clusters based on previous transactions
________
## How to Run

### Prerequisites

Java Development Kit (JDK): Ensure you have JDK version 25 or later installed.

### Instructions

- The JAR files have been included in the repository for ease of use. 
- They already have all dependencies bundled, including gRPC and protobuf libraries.
- Run the java command (replace `<CLI_JAR_WITH_DEPENDECIES_PATH>` with the actual path to the JAR file):
    ```
    java -Dcom.google.protobuf.use_unsafe_pre22_gencode=true --enable-native-access=ALL-UNNAMED --add-exports=java.base/jdk.internal.ref=ALL-UNNAMED --add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED --add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED -Xmx4g -jar <CLI_JAR_WITH_DEPENDECIES_PATH>
    ```


## Credits and Sources
- GRPC docs
- Protobuf docs
- StackOverflow
- Oracle Java docs
- Multi-Paxos Algorithm Paper
- Course content
- Designing Data-Intensive Applications by Martin Kleppmann
- Various online blogs and articles on distributed systems and consensus algorithms

## External Libraries Used
- gRPC for RPC framework
- Protobuf for data serialization
- Jackson for JSON parsing
- opnCSV for CSV parsing
- SLF4j for logging
- Maven for dependency management and build automation
- ChronicleDB for high-performance key-value storage

## Use of AI
- Used LLMs for command-line interface and file handling logic.
- All AI-generated code has been reviewed and tested for correctness.

________
## Notes

**Observations:**
- By this point, I'd become acquainted with Java and spent more time in designing the interleaving of Intra-shard Paxos and Cross-shard 2-phase commit and how it would all working with locking data records
- One of the goals was to achieve higher throughput which really pushed me to make the application highly concurrent (profiling CPU usage while running large loads)
- Benchmarked the system by running transactions with varying amounts of skew (hot accounts) - similar to Smallbank / TPC-C. Average throughput for intra-shard transactions came out to be ~5000 transactions per second on my laptop. Ran upto 30,000 transactions in ~5s.

**Skills I picked up:**
- Identifying and optimizing concurrency bottlenecks
- Cleaner separation of concerns while being tightly connected (via an interface and passing callbacks)
- Graph partitioning to implement regarding algorithm
- Configuration-driven logic to allow for reconfiguration
- Looking through more logs and profiling CPU usage to identify race conditions and slowdowns

**Potential next steps:**
- There are more data structure optimisations I could make in places
- Would like to allow multiple levels of consistency, similar to Cassandra / MongoDB
- Could push for higher throughout for cross-shard transactions.
d 

**Benchmarking stats** showing throughput and latency across different contention levels in transactions - Got to an average of ~5000 intra-shard transactions per second

<img width="800" height="600" alt="Chart showing throughput across different contention levels" src="https://github.com/user-attachments/assets/b3cc0e21-63fd-4b8c-8085-ccea7cebe310" />

<img width="800" height="600" alt="Chart showing latency (min, max and avg) across different contention levels" src="https://github.com/user-attachments/assets/c0007f99-31f9-4b49-8310-cfa3581a2d6f" />
