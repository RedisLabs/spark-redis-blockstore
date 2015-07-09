# spark-redis-blockstore
Apache spack off heap cache block manager over redis

## Configuration
See the following SparkConf settings for getting OFF_HEAP caching to work with redis:
```scala
conf = new SparkConf().
  set("spark.externalBlockStore.blockManager", "org.apache.spark.storage.RedisBlockManager").
  set("spark.redisBlockStore.url", "redis://localhost:6379").
  set("spark.redisBlockStore.keyPrefix", "spark")
```
## Details
This OFF_HEAP block manager enables caching RDD's in an external redis server. Data is cached in simple redis string values.
Once set up you can persist your RDD's in redis like this:
```scala
myRdd.persist(StorageLevel.OFF_HEAP)
```
Tested against Spark v1.4.1 (make sure https://github.com/apache/spark/pull/6702 is applied)

