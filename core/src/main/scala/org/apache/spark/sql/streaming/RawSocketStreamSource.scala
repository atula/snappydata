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

package org.apache.spark.sql.streaming

import scala.reflect.ClassTag

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.Utils

final class RawSocketStreamSource extends StreamPlanProvider {
  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String],
      schema: StructType): RawSocketStreamRelation = {
    new RawSocketStreamRelation(sqlContext, options, schema)
  }
}

final class RawSocketStreamRelation(
    @transient override val sqlContext: SQLContext,
    options: Map[String, String],
    override val schema: StructType)
    extends StreamBaseRelation(options) {

  val hostname: String = options("hostname")
  val port: Int = options.get("port").map(_.toInt).get
  val T = options("T")

  override protected def createRowStream(): DStream[InternalRow] = {
    val encoder = RowEncoder(schema)
    val t: ClassTag[Any] = ClassTag(Utils.getContextOrSparkClassLoader.loadClass(T))
    context.rawSocketStream[Any](hostname, port,
      storageLevel)(t).flatMap(rowConverter.toRows).map(encoder.toRow)
  }
}
