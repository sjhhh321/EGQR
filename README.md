# EGQR



## Quick Start
***
### Initialize Conda Environment
You can create the conda environment using the dependency file we provide.
```语言名
$ conda env create -f environment.yml
```
### Initialize database and Create base queries
You need Java version 11 to run the GDsmith.

You need to place GDsmith.jar into the query_producer folder, and then run the following code. The **num-tries** parameter controls the number of databases, and the **num-queries** parameter controls the number of base queries generated for each database.
```语言名
$ cd query_producer
$ java -jar GDsmith.jar --num-tries 20 --num-queries 1000 --algorithm compared3 composite
```
The above command will produce 20 files in query_producer/logs/composite. If you want to modify the number of clauses in the generated queries, you must change the source code of GDsmith.

### Start System
We start the graph database system using Docker. This is how MemGraph is started：
```语言名
$ docker run -d --name memgraph \
  -p 7685:7687 \ 
  --memory="10g" \
  --restart=on-failure \
  memgraph/memgraph:latest
```
This is RedisGraph:
```语言名
$ docker run -d \
  --name redis-stack-test1 \
  -p 6379:6379 \
  -p 8001:8001 \
  --restart on-failure \
  --memory=10g \
  -e REDISGRAPH_ARGS="MAX_QUEUED_QUERIES 25 TIMEOUT 10000 RESULTSET_SIZE 10000 QUERY_MEM_CAPACITY 1073741824" \
  redis/redis-stack:6.2.6-v7
```
This is Neo4j:
```语言名
$ docker run -d \
  --name neo4j-5.26.8-testing  \
  -p 7478:7474 \
  -p 7681:7687 \
  --memory=12g \
  --cpus=8 \
  -e NEO4J_AUTH=neo4j/testtest \
  -e NEO4J_dbms_memory_heap_initial__size=2G \
  -e NEO4J_dbms_memory_heap_max__size=6G \
  -e NEO4J_dbms_memory_pagecache_size=4G \
  -e NEO4J_dbms_transaction_timeout=30s \
  -e NEO4J_ACCEPT_LICENSE_AGREEMENT=yes \
  --restart=on-failure:3 \
  neo4j:5.26.8
```
This is AgenGraph:
```语言名
$ docker run \
    --name agensgraph \
    -p 5455:5432 \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_PASSWORD=agens \
    -e POSTGRES_DB=agens \
    --shm-size=1g \
    -d \
    skaiworldwide/agensgraph
```
After starting the Docker container, you can configure the corresponding settings in the **configs/config.ini**.


### Start Testing
You just need to run the test file corresponding to the system using Python. For example, for Neo4j: 
```语言名
$ python database_tests/neo4j/TestNeo4j.py.
```



## Monitoring
We use a Lark bot to monitor bug discoveries. You can enable this by setting the configured token under the [lark] section in **configs/config.ini**. Alternatively, you can enter any value if you choose not to use this function.

## Bug List
[MemGraph](https://github.com/memgraph/memgraph/issues?q=is%3Aissue%20state%3Aopen%20author%3Asjhhh321)
[Neo4j](https://github.com/neo4j/neo4j/issues?q=is%3Aissue%20state%3Aopen%20author%3Asjhhh321)
[RedisGraph](https://github.com/RedisGraph/RedisGraph/issues?q=is%3Aissue%20state%3Aopen%20author%3Asjhhh321)
[faklorDB](https://github.com/FalkorDB/FalkorDB/issues?q=is%3Aissue%20state%3Aopen%20author%3Asjhhh321)
