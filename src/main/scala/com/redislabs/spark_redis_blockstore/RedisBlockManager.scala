package org.apache.spark.storage

import java.nio.ByteBuffer
import redis.clients.jedis.Jedis
import org.apache.spark.Logging
import java.io.IOException
import collection.JavaConversions._
import redis.clients.jedis.exceptions.JedisException
import redis.clients.jedis.JedisPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import java.net.URI

private[spark] class RedisBlockManager() extends ExternalBlockManager with Logging {
  
  var jedisPool: JedisPool = _
  var keyPrefix: String = _
  
  override def toString: String = {"ExternalBlockStore-redis"}
  
  override def init(blockManager: BlockManager, executorId: String): Unit = {
    // Create jedis pool
    val poolConfig = new GenericObjectPoolConfig()
    poolConfig.setTestOnCreate(true) // Add extra verification so configuration check below will be more valid
    this.jedisPool = new JedisPool(poolConfig, URI.create(blockManager.conf.get("spark.redisBlockStore.url", "redis://localhost:6379")))

    // Verify configuration by trying to connect
    val conn = this.jedisPool.getResource()
    try {
      conn.connect()
      if (!conn.isConnected()) {
        logError("Failed to connect to Redis, check redis address configuration.")
        throw new IOException("Failed to connect to the Redis, check redis address configuration.")
      }
    } finally {
      conn.close()
    }

    val basePrefix = blockManager.conf.get("spark.redisBlockStore.keyPrefix", "spark")
    val appPrefix = blockManager.conf.get(ExternalBlockStore.FOLD_NAME)
    this.keyPrefix = s"$basePrefix:$appPrefix:$executorId"
  }
  
  def getKey(blockId: BlockId): String = {
    s"${this.keyPrefix}:${blockId.name}"
  }

  override def removeBlock(blockId: BlockId): Boolean = {
    val conn = this.jedisPool.getResource()
    try {
        conn.del(getKey(blockId)) > 0
    } catch {
      case e: JedisException =>
        throw new IOException("Redis error", e) 
    } finally {
      conn.close()      
    }
  }

  override def blockExists(blockId: BlockId): Boolean = {
    val conn = this.jedisPool.getResource()
    try {
      conn.exists(getKey(blockId))
    } catch {
      case e: JedisException =>
        throw new IOException("Redis error", e) 
    } finally {
      conn.close()
    }
  }

  override def putBytes(blockId: BlockId, bytes: ByteBuffer): Unit = {
    val conn = this.jedisPool.getResource()
    try {
      conn.set(getKey(blockId).getBytes(), bytes.array())
    } catch {
      case e: JedisException =>
        throw new IOException("Redis error", e) 
    } finally {
      conn.close()
    }
  }

  override def getBytes(blockId: BlockId): Option[ByteBuffer] = {
    var res: Array[Byte] = null
    val conn = this.jedisPool.getResource()
    try {
      res = conn.get(getKey(blockId).getBytes)
    } catch {
      case e: JedisException =>
        throw new IOException("Redis error", e) 
    } finally {
      conn.close()
    }
    if (res == null)
      return None
    else
      Some(ByteBuffer.wrap(res))
  }

  override def getSize(blockId: BlockId): Long = {
    val conn = this.jedisPool.getResource()
    try {
      conn.strlen(getKey(blockId))
    } catch {
      case e: JedisException =>
        throw new IOException("Redis error", e) 
    } finally {
      conn.close()
    }
  }

  override def shutdown() {
    try {
      val conn = this.jedisPool.getResource()
      try {
        conn.keys(s"${this.keyPrefix}:*").foreach { redisKey =>
          conn.del(redisKey)
        }
      } finally {
        conn.close()
      }
    } finally {
      this.jedisPool.destroy()
    }
  }
}