/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package org.apache.spark.sql.execution.columnar

import java.sql.{Connection, ResultSet, Statement}
import java.util.UUID
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.Random
import scala.util.control.NonFatal

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.sources.ConnectionProperties
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}

/*
Generic class to query column table from SnappyData execution.
 */
class JDBCSourceAsStore(override val connProperties: ConnectionProperties,
    numPartitions: Int) extends ExternalStore {

  @transient
  protected lazy val rand = new Random

  lazy val connectionType = ExternalStoreUtils.getConnectionType(
    connProperties.dialect)


  def getCachedBatchRDD(tableName: String,
      requiredColumns: Array[String],
      sparkContext: SparkContext): RDD[CachedBatch] = {
    new ExternalStorePartitionedRDD(sparkContext, tableName, requiredColumns,
      numPartitions, this)
  }

  override def storeCachedBatch(tableName: String, batch: CachedBatch,
      partitionId: Int = -1, batchId: Option[UUID] = None): Unit = {
    storeCurrentBatch(tableName, batch, batchId.getOrElse(UUID.randomUUID()),
      getPartitionID(tableName, partitionId))
  }

  protected def getPartitionID(tableName: String,
      partitionId: Int = -1): Int = {
    rand.nextInt(numPartitions)
  }

  def storeCurrentBatch(tableName: String, batch: CachedBatch,
      batchId: UUID, partitionId: Int): Unit = {
    tryExecute(tableName, {
      connection =>
        val rowInsertStr = getRowInsertStr(tableName, batch.buffers.length)
        val stmt = connection.prepareStatement(rowInsertStr)
        stmt.setString(1, batchId.toString)
        stmt.setInt(2, partitionId)
        stmt.setInt(3, batch.numRows)
        // TODO: set to null since stats are currently not being used
        // Need to use them for partition/CachedBatch pruning but also
        // add more efficient custom serialization below (shows perf impact)
        // stmt.setBytes(4, SparkSqlSerializer.serialize(batch.stats))
        stmt.setNull(4, java.sql.Types.BLOB)
        var columnIndex = 5
        batch.buffers.foreach(buffer => {
          stmt.setBytes(columnIndex, buffer)
          columnIndex += 1
        })
        stmt.executeUpdate()
        stmt.close()
    }, closeOnSuccess = true, onExecutor = true)
  }

  override def getConnection(id: String, onExecutor: Boolean): Connection = {
    val connProps = if (onExecutor) connProperties.executorConnProps
    else connProperties.connProps
    ConnectionPool.getPoolConnection(id, connProperties.dialect,
      connProperties.poolProps, connProps, connProperties.hikariCP)
  }

  protected val insertStrings: mutable.HashMap[String, String] =
    new mutable.HashMap[String, String]()

  protected def getRowInsertStr(tableName: String, numOfColumns: Int): String = {
    val istr = insertStrings.getOrElse(tableName, {
      lock(makeInsertStmnt(tableName, numOfColumns))
    })
    istr
  }

  protected def makeInsertStmnt(tableName: String, numOfColumns: Int) = {
    if (!insertStrings.contains(tableName)) {
      val s = insertStrings.getOrElse(tableName,
        s"insert into $tableName values(?,?,?,?${",?" * numOfColumns})")
      insertStrings.put(tableName, s)
    }
    insertStrings(tableName)
  }

  protected val insertStmntLock = new ReentrantLock()

  /** Acquires a read lock on the cache for the duration of `f`. */
  protected[sql] def lock[A](f: => A): A = {
    insertStmntLock.lock()
    try f finally {
      insertStmntLock.unlock()
    }
  }
}

abstract class ResultSetIterator[A](conn: Connection,
    stmt: Statement, rs: ResultSet, context: TaskContext)
    extends Iterator[A] with Logging {

  protected final var hasNextValue = true

  context.addTaskCompletionListener { context => close() }

  override final def hasNext: Boolean = hasNextValue

  protected[execution] final def moveNext(): Unit = {
    var success = false
    try {
      // TODO: see if optimization using rs.lightWeightNext
      // and explicit context pop in close possible (was causing trouble)
      success = rs.next()
    } catch {
      case NonFatal(e) => logWarning("Exception iterating resultSet", e)
    } finally {
      if (!success) {
        close()
      }
    }
  }

  final def close() {
    if (!hasNextValue) return
    try {
      // GfxdConnectionWrapper.restoreContextStack(stmt, rs)
      // rs.lightWeightClose()
      rs.close()
    } catch {
      case e: Exception => logWarning("Exception closing resultSet", e)
    }
    try {
      stmt.close()
    } catch {
      case e: Exception => logWarning("Exception closing statement", e)
    }
    try {
      conn.close()
      logDebug("closed connection for task " + context.partitionId())
    } catch {
      case e: Exception => logWarning("Exception closing connection", e)
    }
    hasNextValue = false
  }
}

final class CachedBatchIteratorOnRS(conn: Connection,
    requiredColumns: Array[String],
    stmt: Statement, rs: ResultSet, context: TaskContext)
    extends ResultSetIterator[CachedBatch](conn, stmt, rs, context) {

  private val numCols = requiredColumns.length
  private val colBuffers = new Array[Array[Byte]](numCols)

  // move once to next at the start (or close if no result available)
  moveNext()

  override def next(): CachedBatch = {
    var i = 0
    while (i < numCols) {
      colBuffers(i) = rs.getBytes(i + 1)
      i += 1
    }
    // val stats = SparkSqlSerializer.deserialize[InternalRow](rs.getBytes("stats"))
    val stats: InternalRow = null
    val numRows = rs.getInt("numRows")
    moveNext()
    CachedBatch(numRows, colBuffers, stats)
  }
}

class ExternalStorePartitionedRDD[T: ClassTag](_sc: SparkContext,
    tableName: String, requiredColumns: Array[String],
    numPartitions: Int,
    store: JDBCSourceAsStore)
    extends RDD[CachedBatch](_sc, Nil) {

  override def compute(split: Partition,
      context: TaskContext): Iterator[CachedBatch] = {
    store.tryExecute(tableName, {
      conn =>
        val resolvedName = {
          if (tableName.indexOf(".") <= 0) {
            conn.getSchema + "." + tableName
          } else tableName
        }

        val par = split.index
        val stmt = conn.createStatement()
        val query = "select " + requiredColumns.mkString(", ") +
            s", numRows, stats from $resolvedName where bucketid = $par"
        val rs = stmt.executeQuery(query)
        new CachedBatchIteratorOnRS(conn, requiredColumns, stmt, rs, context)
    }, closeOnSuccess = false, onExecutor = true)
  }

  override protected def getPartitions: Array[Partition] = {
    for (p <- 0 until numPartitions) {
      partitions(p) = new Partition {
        override def index: Int = p
      }
    }
    partitions
  }
}
