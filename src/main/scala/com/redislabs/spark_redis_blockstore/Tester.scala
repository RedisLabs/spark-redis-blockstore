package com.redislabs.spark_redis_blockstore
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import redis.clients.jedis.Jedis

object Tester extends App {
  
  override def main(args: Array[String]) {
    val logFile = "/var/log/syslog" 
    val conf = new SparkConf().
      set("spark.externalBlockStore.blockManager", "org.apache.spark.storage.RedisBlockManager").
      set("spark.redisBlockStore.url", "redis://localhost:6379").
      setMaster("local[2]").setAppName("Simple Application")
    val sc = new SparkContext(conf)
    
    val searchWord1 = "linux"
    val searchWord2 = "cron"
    
    val logData = sc.textFile(logFile, 2).persist(StorageLevel.OFF_HEAP)
    val numWord1 = logData.filter(line => line.contains(searchWord1)).count()
    val numWord2 = logData.filter( line => line.contains(searchWord2)).count()
    println(s"Lines with $searchWord1: $numWord1, lines with $searchWord2: $numWord2")
    
    val counts = logData.flatMap(line => line.split("\\s+"))
                 .map(word => (word, 1))
                 .reduceByKey(_ + _)
    val res = counts.top(10)(Ordering.by(_._2))
    println(s"Top 10:")
    res.foreach(println)
  }
}