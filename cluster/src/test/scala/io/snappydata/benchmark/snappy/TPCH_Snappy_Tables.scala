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
package io.snappydata.benchmark.snappy

import java.io.File

import com.typesafe.config.Config
import io.snappydata.benchmark.{TPCHColumnPartitionedTable, TPCHReplicatedTable}
import spark.jobserver.{SparkJobInvalid, SparkJobValid, SparkJobValidation}

import org.apache.spark.sql.{SnappyJobValid, SnappyJobInvalid, SnappyJobValidation, SnappyContext, SnappySQLJob}

object TPCH_Snappy_Tables extends SnappySQLJob{

   var tpchDataPath: String = _
   var buckets_Order_Lineitem: String = _
   var buckets_Cust_Part_PartSupp: String = _
   var buckets_Nation_Region_Supp: String = _
   var useIndex: Boolean = _
   var nation_Region_Supp_col = false

   override def runSnappyJob(snc: SnappyContext, jobConfig: Config): Any = {
     val props : Map[String, String] = null
     val isSnappy = true


     val usingOptionString = s"""
           USING row
           OPTIONS ()"""

     if (nation_Region_Supp_col) {
       TPCHColumnPartitionedTable.createAndPopulateNationTable(props, snc, tpchDataPath, isSnappy, buckets_Nation_Region_Supp)
       TPCHColumnPartitionedTable.createAndPopulateRegionTable(props, snc, tpchDataPath, isSnappy, buckets_Nation_Region_Supp)
       TPCHColumnPartitionedTable.createAndPopulateSupplierTable(props, snc, tpchDataPath, isSnappy, buckets_Nation_Region_Supp)
     } else {
       TPCHReplicatedTable.createPopulateRegionTable(usingOptionString, props, snc, tpchDataPath, isSnappy)
       TPCHReplicatedTable.createPopulateNationTable(usingOptionString, props, snc, tpchDataPath, isSnappy)
       TPCHReplicatedTable.createPopulateSupplierTable(usingOptionString, props, snc, tpchDataPath, isSnappy)
     }

     TPCHColumnPartitionedTable.createAndPopulateOrderTable(props, snc, tpchDataPath, isSnappy, buckets_Order_Lineitem)
     TPCHColumnPartitionedTable.createAndPopulateLineItemTable(props, snc, tpchDataPath, isSnappy, buckets_Order_Lineitem)
     TPCHColumnPartitionedTable.createPopulateCustomerTable(usingOptionString, props, snc, tpchDataPath, isSnappy, buckets_Cust_Part_PartSupp)
     TPCHColumnPartitionedTable.createPopulatePartTable(usingOptionString, props, snc, tpchDataPath, isSnappy, buckets_Cust_Part_PartSupp)
     TPCHColumnPartitionedTable.createPopulatePartSuppTable(usingOptionString, props, snc, tpchDataPath, isSnappy, buckets_Cust_Part_PartSupp)
     if (useIndex) {
       TPCHColumnPartitionedTable.createAndPopulateOrder_CustTable(props, snc, tpchDataPath, isSnappy, buckets_Cust_Part_PartSupp)
       TPCHColumnPartitionedTable.createAndPopulateLineItem_partTable(props, snc, tpchDataPath, isSnappy, buckets_Cust_Part_PartSupp)
     }
   }

   override def isValidJob(sc: SnappyContext, config: Config): SnappyJobValidation = {

     tpchDataPath = if (config.hasPath("dataLocation")) {
       config.getString("dataLocation")
     } else {
       "/QASNAPPY/TPCH/DATA/1"
     }

     buckets_Order_Lineitem = if (config.hasPath("Buckets_Order_Lineitem")) {
       config.getString("Buckets_Order_Lineitem")
     } else {
       "15"
     }


     buckets_Cust_Part_PartSupp = if (config.hasPath("Buckets_Cust_Part_PartSupp")) {
       config.getString("Buckets_Cust_Part_PartSupp")
     } else {
       "15"
     }

     buckets_Nation_Region_Supp = if (config.hasPath("Buckets_Nation_Region_Supp")) {
       config.getString("Buckets_Nation_Region_Supp")
     } else {
       "3"
     }

     nation_Region_Supp_col = if (config.hasPath("Nation_Region_Supp_col")) {
       config.getBoolean("Nation_Region_Supp_col")
     } else {
       false
     }

     if (!(new File(tpchDataPath)).exists()) {
       return new SnappyJobInvalid("Incorrect tpch data path. " +
           "Specify correct location")
     }

     useIndex = if (config.hasPath("useIndex")) {
       config.getBoolean("useIndex")
     } else {
       return new SnappyJobInvalid("Specify whether to use Index")
     }
     SnappyJobValid()
   }
 }
