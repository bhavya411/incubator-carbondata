/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.File
import java.sql.ResultSet

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

case class Query1(sqlText: String)

// case class HiveOrcTablePerformance(tableName: String, query: String, time: String)

//case class HiveCarbonTablePerformance(tableName: String, query: String, time: String)


object CreateCarbonTableTPCH {

  val hiveCarbonTableName = "comparetest_hive_carbon"
  val hiveOrcTableName = "hivetable"

  val logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  val rootPath = new File(this.getClass.getResource("/").getPath
                          + "../../../..").getCanonicalPath
  val warehouse = s"$rootPath/integration/hive/target/warehouse"
  val metastoredb = s"/home/bhavya/sparktest/comparetest_metastore_db"

  val carbonTableName = "comparetest_hive_carbon"

  var resultSet: ResultSet = _

  def main(args: Array[String]) {
    CarbonProperties.getInstance()
      .addProperty("carbon.enable.vector.reader", "true")
      .addProperty("enable.unsafe.sort", "true")
      .addProperty("carbon.blockletgroup.size.in.mb", "32")
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")
      .addProperty(CarbonCommonConstants.LOCK_TYPE, CarbonCommonConstants.CARBON_LOCK_TYPE_HDFS)


    import org.apache.spark.sql.CarbonSession._


    val carbon = SparkSession
      .builder()
      .master("local")
      .appName("CompareTestExample")
      .config("carbon.sql.warehouse.dir", warehouse).enableHiveSupport()
      .getOrCreateCarbonSession(
        "hdfs://localhost:54311/opt/tpch5gb", metastoredb)


    /*
    val hiveEmbeddedServer2 = new HiveEmbeddedServer2
    hiveEmbeddedServer2.start()
    val port = hiveEmbeddedServer2.getFreePort

    Try(Class.forName("org.apache.hive.jdbc.HiveDriver")).getOrElse(
      throw new DatastoreDriverNotFoundException("driver not found "))

    val con = DriverManager
      .getConnection(s"jdbc:hive2://localhost:$port/default", "anonymous", "anonymous")
    val stmt = con.createStatement

    println(s"============HIVE CLI IS STARTED ON PORT $port ==============")


    val tableList = Map("orders" -> "ORDERS","customer" -> "CUSTOMER",
      "lineitem" -> "LINEITEM","nation" -> "NATION","region" -> "REGION",
      "part" -> "PART","supplier"->"SUPPLIER", "partsupp" -> "PARTSUPP")

    val orcTableList = tableList.map{ case (x,y) => (x,y +"_ORC")}

    tableList.values.map{ x => stmt.execute("DROP TABLE IF EXISTS " + x) }
    */

/*

    carbon.sql("DROP TABLE IF EXISTS NATION")

    carbon
      .sql(
        "create table if not exists NATION ( N_NAME string, N_NATIONKEY string, N_REGIONKEY " +
        "string, N_COMMENT string ) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES " +
        "('DICTIONARY_EXCLUDE'='N_COMMENT', 'table_blocksize'='128')")

    carbon.sql("LOAD DATA INPATH 'hdfs://localhost:54311/user2/nation.csv' INTO TABLE nation " +
               "OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='\"','FILEHEADER'='N_NATIONKEY,N_NAME," +
               "N_REGIONKEY,N_COMMENT')")

    carbon.sql("DROP TABLE IF EXISTS REGION")

    carbon
      .sql(
        "create table if not exists REGION( R_NAME string, R_REGIONKEY string, R_COMMENT string )" +
        " STORED BY 'org.apache.carbondata.format' TBLPROPERTIES " +
        "('DICTIONARY_EXCLUDE'='R_COMMENT', 'table_blocksize'='128')")

    carbon.sql("LOAD DATA INPATH \"hdfs://localhost:54311/user2/region.csv\" INTO TABLE region " +
               "OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='\"','FILEHEADER'='R_REGIONKEY,R_NAME," +
               "R_COMMENT')")

    carbon.sql("DROP TABLE IF EXISTS PART")

    carbon
      .sql(
        "create table if not exists PART( P_BRAND string, P_SIZE int, P_CONTAINER string, P_TYPE " +
        "string, P_PARTKEY string, P_NAME string, P_MFGR string, P_RETAILPRICE double, P_COMMENT " +
        "string ) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES " +
        "('DICTIONARY_INCLUDE'='P_SIZE','DICTIONARY_EXCLUDE'='P_PARTKEY, P_NAME, P_COMMENT', " +
        "'table_blocksize'='128')")

    carbon
      .sql("LOAD DATA INPATH \"hdfs://localhost:54311/user2/part.csv\" INTO TABLE part OPTIONS" +
           "('DELIMITER'='|' , 'QUOTECHAR'='\"','FILEHEADER'='P_PARTKEY,P_NAME,P_MFGR,P_BRAND," +
           "P_TYPE,P_SIZE,P_CONTAINER,P_RETAILPRICE,P_COMMENT')")

    carbon.sql("DROP TABLE IF EXISTS SUPPLIER ")

    carbon
      .sql(
        "create table if not exists SUPPLIER( S_COMMENT string, S_SUPPKEY string, S_NAME string, " +
        "S_ADDRESS string, S_NATIONKEY string, S_PHONE string, S_ACCTBAL double ) STORED BY 'org" +
        ".apache.carbondata.format' TBLPROPERTIES ('DICTIONARY_EXCLUDE'='S_COMMENT, S_SUPPKEY, " +
        "S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE' , 'table_blocksize'='128')")

    carbon
      .sql("LOAD DATA INPATH \"hdfs://localhost:54311/user2/supplier.csv\" INTO TABLE supplier " +
           "OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='\"','FILEHEADER'=' S_SUPPKEY,             " +
           "S_NAME,S_ADDRESS,S_NATIONKEY,S_PHONE,S_ACCTBAL,S_COMMENT')")

    carbon.sql("DROP TABLE IF EXISTS PARTSUPP ")

    carbon
      .sql(
        "create table if not exists PARTSUPP ( PS_PARTKEY string, PS_SUPPKEY string, PS_AVAILQTY " +
        "int, PS_SUPPLYCOST double, PS_COMMENT string ) STORED BY 'org.apache.carbondata.format' " +
        "TBLPROPERTIES ('DICTIONARY_EXCLUDE'='PS_PARTKEY, PS_SUPPKEY, PS_COMMENT', " +
        "'table_blocksize'='128')")


    carbon
      .sql("LOAD DATA INPATH \"hdfs://localhost:54311/user2/partsupp.csv\" INTO TABLE partsupp " +
           "OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='\"','FILEHEADER'='PS_PARTKEY,PS_SUPPKEY ," +
           "PS_AVAILQTY,PS_SUPPLYCOST,PS_COMMENT')")

    carbon.sql("DROP TABLE IF EXISTS CUSTOMER")

    carbon
      .sql(
        "create table if not exists CUSTOMER( C_MKTSEGMENT string, C_NATIONKEY string, C_CUSTKEY " +
        "string, C_NAME string, C_ADDRESS string, C_PHONE string, C_ACCTBAL double, C_COMMENT " +
        "string ) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES " +
        "('DICTIONARY_EXCLUDE'='C_CUSTKEY,C_NAME,C_ADDRESS,C_PHONE,C_COMMENT', " +
        "'table_blocksize'='128')")

    carbon
      .sql("LOAD DATA INPATH \"hdfs://localhost:54311/user2/customer.csv\" INTO TABLE customer " +
           "OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='\"' , 'FILEHEADER'='C_CUSTKEY,C_NAME," +
           "C_ADDRESS,C_NATIONKEY,C_PHONE,C_ACCTBAL,C_MKTSEGMENT,C_COMMENT')")


    carbon.sql("DROP TABLE IF EXISTS ORDERS ")


    carbon
      .sql(
        "create table if not exists ORDERS( O_ORDERDATE date, O_ORDERPRIORITY string, " +
        "O_ORDERSTATUS string, O_ORDERKEY string, O_CUSTKEY string, O_TOTALPRICE double, O_CLERK " +
        "string, O_SHIPPRIORITY int, O_COMMENT string ) STORED BY 'org.apache.carbondata.format' " +
        "TBLPROPERTIES ('DICTIONARY_EXCLUDE'='O_ORDERKEY, O_CUSTKEY, O_CLERK, O_COMMENT', " +
        "'table_blocksize'='128','no_inverted_index'='O_ORDERKEY, O_CUSTKEY, O_CLERK, O_COMMENT')")

    carbon.sql("LOAD DATA INPATH 'hdfs://localhost:54311/user2/orders.csv' INTO TABLE orders " +
               "OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='\"','FILEHEADER'='O_ORDERKEY,O_CUSTKEY," +
               "O_ORDERSTATUS,O_TOTALPRICE,O_ORDERDATE,O_ORDERPRIORITY,O_CLERK,O_SHIPPRIORITY," +
               "O_COMMENT')")


    carbon.sql("DROP TABLE IF EXISTS LINEITEM")


    carbon
      .sql(
        "create table if not exists lineitem( L_SHIPDATE date, L_SHIPMODE string, L_SHIPINSTRUCT " +
        "string, L_RETURNFLAG string, L_RECEIPTDATE date, L_ORDERKEY string, L_PARTKEY string, " +
        "L_SUPPKEY string, L_LINENUMBER int, L_QUANTITY double, L_EXTENDEDPRICE double, " +
        "L_DISCOUNT double, L_TAX double, L_LINESTATUS string, L_COMMITDATE date, L_COMMENT " +
        "string ) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES " +
        "('DICTIONARY_EXCLUDE'='L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_COMMENT', " +
        "'table_blocksize'='128', 'no_inverted_index'='L_ORDERKEY, L_PARTKEY, L_SUPPKEY, " +
        "L_COMMENT')")

    carbon.sql("LOAD DATA INPATH 'hdfs://localhost:54311/user2/lineitem.csv' INTO TABLE lineitem " +
               "OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='\"','FILEHEADER'=' L_ORDERKEY,L_PARTKEY," +
               "L_SUPPKEY,L_LINENUMBER,L_QUANTITY,L_EXTENDEDPRICE,L_DISCOUNT,L_TAX,L_RETURNFLAG," +
               "L_LINESTATUS,L_SHIPDATE,L_COMMITDATE,L_RECEIPTDATE,L_SHIPINSTRUCT,L_SHIPMODE," +
               "L_COMMENT')")
*/
    carbon.sql("select * from orders where o_orderdate = cast('1992-06-05' as date)").show()
    /*

    carbon.sql("show tables").show()


    logger.info("Creating Hive Tables")


    //Create corresponding Tables in Hive for Carbon Tables

    stmt.execute("CREATE TABLE NATION  ( " +
                 "N_NATIONKEY  INT," +
                 "N_NAME STRING," +
                 "N_REGIONKEY  INT," +
                 "N_COMMENT STRING)")

    stmt
      .execute(
        "ALTER TABLE NATION SET FILEFORMAT INPUTFORMAT \"org.apache.carbondata." +
        "hive.MapredCarbonInputFormat\"OUTPUTFORMAT \"org.apache.carbondata.hive." +
        "MapredCarbonOutputFormat\"SERDE \"org.apache.carbondata.hive." +
        "CarbonHiveSerDe\" ")
    stmt
      .execute(
        "ALTER TABLE NATION SET LOCATION " +
        s"'hdfs://localhost:54311/opt/tpch/default/nation' ".stripMargin)

    stmt.execute("CREATE TABLE REGION ( R_REGIONKEY INT ,\n R_NAME STRING ,\n R_COMMENT STRING)")

    stmt
      .execute(
        "ALTER TABLE REGION SET FILEFORMAT INPUTFORMAT \"org.apache.carbondata." +
        "hive.MapredCarbonInputFormat\"OUTPUTFORMAT \"org.apache.carbondata.hive." +
        "MapredCarbonOutputFormat\"SERDE \"org.apache.carbondata.hive." +
        "CarbonHiveSerDe\" ")

    stmt
      .execute(
        "ALTER TABLE REGION SET LOCATION " +
        s"'hdfs://localhost:54311/opt/tpch/default/region' ".stripMargin)

    stmt
      .execute(
        "CREATE TABLE PART ( P_PARTKEY INT ,\n P_NAME STRING ,\n P_MFGR STRING ,\n P_BRAND " +
        "STRING ,\n P_TYPE STRING ,\n P_SIZE INT ,\n P_CONTAINER STRING ,\n P_RETAILPRICE " +
        "DECIMAL(15,2) ,\n P_COMMENT STRING )")

    stmt
      .execute(
        "ALTER TABLE PART SET FILEFORMAT INPUTFORMAT \"org.apache.carbondata." +
        "hive.MapredCarbonInputFormat\"OUTPUTFORMAT \"org.apache.carbondata.hive." +
        "MapredCarbonOutputFormat\"SERDE \"org.apache.carbondata.hive." +
        "CarbonHiveSerDe\" ")

    stmt
      .execute(
        "ALTER TABLE PART SET LOCATION " +
        s"'hdfs://localhost:54311/opt/tpch/default/part' ".stripMargin)


    stmt
      .execute("CREATE TABLE SUPPLIER ( S_SUPPKEY INT ,\n S_NAME STRING ,\n S_ADDRESS STRING ,\n " +
               "S_NATIONKEY INT ,\n S_PHONE STRING ,\n S_ACCTBAL DECIMAL(15,2) ,\n S_COMMENT " +
               "STRING )")

    stmt
      .execute(
        "ALTER TABLE SUPPLIER SET FILEFORMAT INPUTFORMAT \"org.apache.carbondata." +
        "hive.MapredCarbonInputFormat\"OUTPUTFORMAT \"org.apache.carbondata.hive." +
        "MapredCarbonOutputFormat\"SERDE \"org.apache.carbondata.hive." +
        "CarbonHiveSerDe\" ")
    stmt
      .execute(
        "ALTER TABLE SUPPLIER SET LOCATION " +
        s"'hdfs://localhost:54311/opt/tpch/default/supplier' ".stripMargin)

    stmt
      .execute(
        "CREATE TABLE PARTSUPP ( PS_PARTKEY INT ,\n PS_SUPPKEY INT ,\n PS_AVAILQTY INT ,\n " +
        "PS_SUPPLYCOST DECIMAL(15,2) ,\n PS_COMMENT STRING )")

    stmt
      .execute(
        "ALTER TABLE PARTSUPP SET FILEFORMAT INPUTFORMAT \"org.apache.carbondata." +
        "hive.MapredCarbonInputFormat\"OUTPUTFORMAT \"org.apache.carbondata.hive." +
        "MapredCarbonOutputFormat\"SERDE \"org.apache.carbondata.hive." +
        "CarbonHiveSerDe\" ")
    stmt
      .execute(
        "ALTER TABLE PARTSUPP SET LOCATION " +
        s"'hdfs://localhost:54311/opt/tpch/default/partsupp' ".stripMargin)

    stmt
      .execute(
        "CREATE TABLE ORDERS ( O_ORDERKEY INT ,O_CUSTKEY INT ,O_ORDERSTATUS STRING ,O_TOTALPRICE DECIMAL(15,2) , O_ORDERDATE DATE ,O_ORDERPRIORITY STRING , O_CLERK STRING , O_SHIPPRIORITY INT ,O_COMMENT STRING )")

    stmt
      .execute(
        "ALTER TABLE ORDERS SET FILEFORMAT INPUTFORMAT \"org.apache.carbondata." +
        "hive.MapredCarbonInputFormat\"OUTPUTFORMAT \"org.apache.carbondata.hive." +
        "MapredCarbonOutputFormat\"SERDE \"org.apache.carbondata.hive." +
        "CarbonHiveSerDe\" ")

    stmt.execute("ALTER TABLE ORDERS SET LOCATION 'hdfs://localhost:54311/opt/tpch/default/orders'")

    stmt
      .execute(
        "CREATE TABLE CUSTOMER ( C_CUSTKEY INT ,C_NAME STRING ,C_ADDRESS STRING ,C_NATIONKEY INT ,C_PHONE STRING ,C_ACCTBAL DECIMAL(15,2) ,C_MKTSEGMENT STRING ,C_COMMENT STRING )")

    stmt
      .execute(
        "ALTER TABLE CUSTOMER SET FILEFORMAT INPUTFORMAT \"org.apache.carbondata." +
        "hive.MapredCarbonInputFormat\"OUTPUTFORMAT \"org.apache.carbondata.hive." +
        "MapredCarbonOutputFormat\"SERDE \"org.apache.carbondata.hive." +
        "CarbonHiveSerDe\" ")

    stmt
      .execute(
        "ALTER TABLE CUSTOMER SET LOCATION 'hdfs://localhost:54311/opt/tpch/default/customer'")

    stmt
      .execute(
        " CREATE TABLE LINEITEM ( L_ORDERKEY INT ,L_PARTKEY INT ,L_SUPPKEY INT , L_LINENUMBER INT ,L_QUANTITY DECIMAL(15,2) ,L_EXTENDEDPRICE DECIMAL(15,2) ,L_DISCOUNT DECIMAL(15,2) ,L_TAX DECIMAL(15,2) ,L_RETURNFLAG STRING , L_LINESTATUS STRING ,L_SHIPDATE DATE ,L_COMMITDATE DATE ,L_RECEIPTDATE DATE ,L_SHIPINSTRUCT STRING ,L_SHIPMODE STRING ,L_COMMENT STRING )")

    stmt
      .execute(
        "ALTER TABLE LINEITEM SET FILEFORMAT INPUTFORMAT \"org.apache.carbondata." +
        "hive.MapredCarbonInputFormat\"OUTPUTFORMAT \"org.apache.carbondata.hive." +
        "MapredCarbonOutputFormat\"SERDE \"org.apache.carbondata.hive." +
        "CarbonHiveSerDe\" ")


    stmt
      .execute(
        "ALTER TABLE LINEITEM SET LOCATION 'hdfs://localhost:54311/opt/tpch/default/lineitem'")

*/
/*
    val hive_Carbon_Result = new ListBuffer[HiveCarbonTablePerformance]()
    val hive_carbon_resultSet = new ListBuffer[(ResultSet)]()



    val hive_Orc_Result = new ListBuffer[HiveOrcTablePerformance]()
    val hive_orc_resultSet = new ListBuffer[(ResultSet)]()


    //execute select queries to get timing these are hive table queries
    getQueries(tableList).zipWithIndex.foreach {
      case (query, index) =>
        val rt = time {
          println("************************EXECUTIED QUERY ***********************" + query.sqlText)
          resultSet = stmt.executeQuery(query.sqlText)
        }
        hive_carbon_resultSet += resultSet
        println("time taken by Carbon on hive")
        println(s"**************=> $rt sec ********************")
        hive_Carbon_Result += HiveCarbonTablePerformance(query.sqlText, rt.toString, index.toString)
    }
    */
    // now start reading orc tables  first dropping hive tables

/*
    stmt.execute(s"DROP TABLE IF EXISTS NATION_ORC")

    stmt.execute("CREATE TABLE NATION_ORC (N_NATIONKEY INT,\n N_NAME STRING,\n N_REGIONKEY INT,\n " +
                 "N_COMMENT STRING) stored as orc ")

    stmt.execute("INSERT INTO NATION_ORC SELECT * FROM NATION")


    stmt.execute(s"DROP TABLE IF EXISTS REGION_ORC")


    stmt
      .execute(
        "CREATE TABLE REGION_ORC (R_REGIONKEY INT , R_NAME STRING , R_COMMENT STRING) STORED AS ORC ")

    stmt.execute("INSERT INTO REGION_ORC SELECT * FROM REGION")

    stmt.execute(s"DROP TABLE IF EXISTS PART_ORC")

    stmt.execute("CREATE TABLE PART_ORC ( P_PARTKEY INT ,P_NAME STRING ,P_MFGR STRING ," +
                 "P_BRAND STRING ,P_TYPE STRING ,P_SIZE INT ,P_CONTAINER STRING , " +
                 "P_RETAILPRICE DECIMAL(15,2) ,P_COMMENT STRING ) STORED AS ORC ")

    stmt.execute("INSERT INTO PART_ORC SELECT * FROM PART")

    stmt.execute(s"DROP TABLE IF EXISTS SUPPLIER_ORC")

    stmt
      .execute(
        "CREATE TABLE SUPPLIER_ORC ( S_SUPPKEY INT ,S_NAME STRING ,S_ADDRESS STRING , " +
        "S_NATIONKEY INT , S_PHONE STRING , S_ACCTBAL DECIMAL(15,2) , S_COMMENT STRING ) " +
        "STORED AS ORC ")

    stmt.execute("INSERT INTO SUPPLIER_ORC SELECT * FROM SUPPLIER")

    stmt.execute(s"DROP TABLE IF EXISTS PARTSUPP_ORC")

    stmt.execute("CREATE TABLE PARTSUPP_ORC ( PS_PARTKEY INT ,PS_SUPPKEY INT ,PS_AVAILQTY INT ," +
                 "PS_SUPPLYCOST DECIMAL(15,2) ,PS_COMMENT STRING ) STORED AS ORC")

    stmt.execute("INSERT INTO PARTSUPP_ORC SELECT * FROM PARTSUPP")

    stmt.execute(s"DROP TABLE IF EXISTS CUSTOMER_ORC")

    stmt.execute("CREATE TABLE CUSTOMER_ORC ( C_CUSTKEY INT ,C_NAME STRING ,C_ADDRESS STRING ," +
                 "C_NATIONKEY INT ,C_PHONE STRING ,C_ACCTBAL DECIMAL(15,2) ,C_MKTSEGMENT " +
                 "STRING ,C_COMMENT STRING)STORED AS ORC ")

    stmt.execute("INSERT INTO CUSTOMER_ORC SELECT * FROM CUSTOMER")

    stmt.execute(s"DROP TABLE IF EXISTS ORDERS_ORC")

    stmt
      .execute(
        "CREATE TABLE ORDERS_ORC ( O_ORDERKEY INT ,O_CUSTKEY INT ,O_ORDERSTATUS STRING , " +
        " O_TOTALPRICE DECIMAL(15,2) , O_ORDERDATE DATE , O_ORDERPRIORITY STRING , " +
        "O_CLERK STRING, O_SHIPPRIORITY INT , O_COMMENT STRING )STORED AS ORC")

    stmt.execute("INSERT INTO ORDERS_ORC SELECT * FROM ORDERS")

    stmt.execute(s"DROP TABLE IF EXISTS LINEITEM_ORC")

    stmt.execute("CREATE TABLE LINEITEM_ORC( L_ORDERKEY INT ,L_PARTKEY INT ,L_SUPPKEY INT ," +
                 "L_LINENUMBER INT ,L_QUANTITY DECIMAL(15,2)," +
                 "L_EXTENDEDPRICE DECIMAL(15,2) ,L_DISCOUNT DECIMAL" +
                 "(15,2),L_TAX DECIMAL(15,2) ,L_RETURNFLAG STRING," +
                 " L_LINESTATUS STRING ,L_SHIPDATE DATE ,L_COMMITDATE DATE" +
                 " ,L_RECEIPTDATE DATE ,L_SHIPINSTRUCT STRING" +
                 " ,L_SHIPMODE STRING ,L_COMMENT STRING ) STORED AS ORC")

    stmt.execute("INSERT INTO LINEITEM_ORC SELECT * FROM LINEITEM")
*/
    //execute select queries to get timing these are hive orc table queries

/*
    getQueries(orcTableList).zipWithIndex.foreach {
      case (query, index) =>
        val queryExecutionTime = time {
          println(
            "************************EXECUTIED QUERY ***********************" + query.sqlText)
          resultSet = stmt.executeQuery(query.sqlText)
        }
        hive_orc_resultSet += resultSet
        println("time taken by Carbon on hive")
        println(s"**************=> $queryExecutionTime sec ********************")
        hive_Orc_Result +=
        HiveOrcTablePerformance(query.sqlText, queryExecutionTime.toString, index.toString)
    }


    println(s"| QUERY |" + s"|  ORC Execution Time                    ||" +
            s"      Carbon Execution Time           |" + s"${}")
    println("+---+" +
            "+------------++------------------------------------------------------------------------------------------------------------------------------+")

    for (i <- hive_Carbon_Result.indices) {
      val hiveExecutionTime = hive_Orc_Result(i).time
      val hive_CarbonExecutionTime = hive_Carbon_Result(i).time
      val queryIndex = i + 1

      println(s"| $queryIndex |" + s"| $hiveExecutionTime                     ||" +
              s"      $hive_CarbonExecutionTime               |" + s"${}")
      println("+---+" +
              "+------------++------------------------------------------------------------------------------------------------------------------------------+")

    } */

    carbon.stop()

    System.exit(0)
  }





  private def getQueries(tableNames : Map[String,String]): Array[Query1] = {
    Array(
      // ===========================================================================
      // ==                     FULL SCAN AGGREGATION                             ==
      // ===========================================================================
      //failed

      Query1(
        s"select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as " +
        "sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum" +
        "(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as " +
        "avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as " +
        s"count_order from ${tableNames.get("lineitem").get} where l_shipdate <= '1998-09-16' group by l_returnflag, " +
        "l_linestatus order by l_returnflag, l_linestatus"
      ),
      Query1(
        s"select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, " +
        s"o_shippriority from ${tableNames.get("customer").get}, ${tableNames.get("orders").get}, ${tableNames.get("lineitem").get} where c_mktsegment = 'BUILDING' and " +
        "c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < '1995-03-22' and " +
        "l_shipdate > '1995-03-22' group by l_orderkey, o_orderdate, o_shippriority order by " +
        "revenue desc, o_orderdate limit 10"
      ),

      Query1(
        "select\n        o_orderpriority,\n        count(*) as order_count\nfrom\n " +
        s" ${tableNames.get("orders").get} " +
        "as o\nwhere\n        o_orderdate >= '1996-05-01'\n        and o_orderdate < " +
        "'1996-08-01'\n        and exists (\n                select\n                        *\n " +
        s"               from\n                         ${tableNames.get("lineitem").get}\n                where\n          " +
        "              l_orderkey = o.o_orderkey\n                        and l_commitdate < " +
        "l_receiptdate\n        )\ngroup by\n        o_orderpriority\norder by\n        " +
        "o_orderpriority"
      ),
       Query1(
         "select\n        n_name,\n        sum(l_extendedprice * (1 - l_discount)) as " +
         s"revenue\nfrom\n    ${tableNames.get("customer").get}     ,\n      ${tableNames.get("orders").get}   ,\n     ${tableNames.get("lineitem").get}    ,\n     ${tableNames.get("supplier").get}    ," +
         "\n        nation,\n        region\nwhere\n        c_custkey = o_custkey\n        and " +
         "l_orderkey = o_orderkey\n        and l_suppkey = s_suppkey\n        and c_nationkey = " +
         "s_nationkey\n        and s_nationkey = n_nationkey\n        and n_regionkey = " +
         "r_regionkey\n        and r_name = 'AFRICA'\n        and o_orderdate >= '1993-01-01'\n   " +
         "     and o_orderdate < '1994-01-01'\ngroup by\n        n_name\norder by\n        revenue" +
         " desc"
       ),
      Query1(
        s"select sum(l_extendedprice * l_discount) as revenue from  ${tableNames.get("lineitem").get}  where l_shipdate >= " +
        "'1993-01-01' and l_shipdate < '1994-01-01' and l_discount between 0.06 - 0.01 and 0.06 +" +
        " 0.01 and l_quantity < 25"
      ),
      Query1(
        "select supp_nation, cust_nation, l_year, sum(volume) as revenue from ( select n1.n_name " +
        "as supp_nation, n2.n_name as cust_nation, year(l_shipdate) as l_year, l_extendedprice * " +
        s"(1 - l_discount) as volume from  ${ tableNames.get("supplier").get } ,  ${
          tableNames
            .get("lineitem").get
        } ,  ${ tableNames.get("orders").get } , ${ tableNames.get("customer").get} , ${
          tableNames
            .get("nation").get
        } n1, ${ tableNames.get("nation").get } " +
        "n2 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and" +
        " s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey and ( (n1.n_name = " +
        "'KENYA' and n2.n_name = 'PERU') or (n1.n_name = 'PERU' and n2.n_name = 'KENYA') ) and " +
        "l_shipdate between '1995-01-01' and '1996-12-31' ) as shipping group by supp_nation, " +
        "cust_nation, l_year order by supp_nation, cust_nation, l_year"),

      Query1(
           "select o_year, sum(case when nation = 'PERU' then volume else 0 end) / sum(volume) as " +
           "mkt_share from ( select year(o_orderdate) as o_year, l_extendedprice * (1 - l_discount) " +
           s"as volume, n2.n_name as nation from  ${ tableNames.get("part").get}, ${ tableNames.get("supplier").get } , ${ tableNames.get("lineitem").get }, ${ tableNames.get("orders").get }, ${ tableNames.get("customer").get }, ${ tableNames.get("nation").get } " +
           "n1, nation n2, region where p_partkey = l_partkey and s_suppkey = l_suppkey and " +
           "l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and " +
           "n1.n_regionkey = r_regionkey and r_name = 'AMERICA' and s_nationkey = n2.n_nationkey and" +
           " o_orderdate between '1995-01-01' and '1996-12-31' and p_type = 'ECONOMY BURNISHED " +
           "NICKEL' ) as all_nations group by o_year order by o_year"),

      Query1(
        "select nation, o_year, sum(amount) as sum_profit from ( select n_name as nation, year" +
        "(o_orderdate) as o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity" +
        s" as amount from ${ tableNames.get("part").get }, ${ tableNames.get("supplier").get }, ${ tableNames.get("lineitem").get }, ${ tableNames.get("partsupp").get }, ${ tableNames.get("orders").get }, ${ tableNames.get("nation").get } where s_suppkey = " +
        "l_suppkey and ps_suppkey = l_suppkey and ps_partkey = l_partkey and p_partkey = " +
        "l_partkey and o_orderkey = l_orderkey and s_nationkey = n_nationkey and p_name like " +
        "'%plum%' ) as profit group by nation, o_year order by nation, o_year desc"),

      Query1(
        "select c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal," +
        s" n_name, c_address, c_phone, c_comment from ${ tableNames.get("customer").get }, ${ tableNames.get("orders").get }, ${ tableNames.get("lineitem").get }, ${ tableNames.get("nation").get } where " +
        "c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= '1993-07-01' and " +
        "o_orderdate < '1993-10-01' and l_returnflag = 'R' and c_nationkey = n_nationkey group by" +
        " c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment order by revenue " +
        "desc limit 20"),

      Query1(
        "select l_shipmode, sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = " +
        "'2-HIGH' then 1 else 0 end) as high_line_count, sum(case when o_orderpriority <> " +
        "'1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count from " +
        s"${ tableNames.get("orders").get }, ${ tableNames.get("lineitem").get } where o_orderkey = l_orderkey and l_shipmode in ('REG AIR', 'MAIL') and" +
        " l_commitdate < l_receiptdate and l_shipdate < l_commitdate and l_receiptdate >= " +
        "'1995-01-01' and l_receiptdate < '1996-01-01' group by l_shipmode order by l_shipmode"),

      Query1(
        "select c_count, count(*) as custdist from ( select c_custkey, count(o_orderkey) as " +
        s"c_count from ${ tableNames.get("customer").get } left outer join ${ tableNames.get("orders").get } on c_custkey = o_custkey and o_comment not " +
        "like '%unusual%accounts%' group by c_custkey ) c_orders group by c_count order by " +
        "custdist desc, c_count desc"
      ),
      //failed
      Query1(
        "select p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt from " +
        s"${ tableNames.get("partsupp").get}, ${ tableNames.get("part").get} where p_partkey = ps_partkey and p_brand <> 'Brand#34' and p_type not " +
        s"like 'ECONOMY BRUSHED%' and p_size in (22, 14, 27, 49, 21, 33, 35, 28) and ${ tableNames.get("partsupp").get}" +
        s".ps_suppkey not in ( select s_suppkey from ${ tableNames.get("supplier").get} where s_comment like " +
        "'%Customer%Complaints%' ) group by p_brand, p_type, p_size order by supplier_cnt desc, " +
        "p_brand, p_type, p_size"
      ),

      Query1(
        s"with q17_part as ( select p_partkey from ${ tableNames.get("part").get} where p_brand = 'Brand#23' and p_container" +
        " = 'MED BOX' ), q17_avg as ( select l_partkey as t_partkey, 0.2 * avg(l_quantity) as " +
        s"t_avg_quantity from ${ tableNames.get("lineitem").get} where l_partkey IN (select p_partkey from q17_part) group " +
        "by l_partkey ), q17_price as ( select l_quantity, l_partkey, l_extendedprice from " +
        "lineitem where l_partkey IN (select p_partkey from q17_part) ) select cast(sum" +
        "(l_extendedprice) / 7.0 as decimal(32,2)) as avg_yearly from q17_avg, q17_price where " +
        "t_partkey = l_partkey and l_quantity < t_avg_quantity"
      ),
      // ===========================================================================
      // ==                      FULL SCAN GROUP BY AGGREGATE                     ==
      // ===========================================================================
      Query1(
        s"with tmp1 as ( select p_partkey from ${ tableNames.get("part").get} where p_name like 'forest%' ), tmp2 as (select" +
        s" s_name, s_address, s_suppkey from ${ tableNames.get("supplier").get}, ${ tableNames.get("nation").get} where s_nationkey = n_nationkey and " +
        "n_name = 'CANADA' ), tmp3 as ( select l_partkey, 0.5 * sum(l_quantity) as sum_quantity, " +
        s"l_suppkey from ${ tableNames.get("lineitem").get}, tmp2 where l_shipdate >= '1994-01-01' and l_shipdate <= " +
        "'1995-01-01' and l_suppkey = s_suppkey group by l_partkey, l_suppkey ), tmp4 as ( select" +
        " ps_partkey, ps_suppkey, ps_availqty from partsupp where ps_partkey IN (select p_partkey" +
        " from tmp1) ), tmp5 as ( select ps_suppkey from tmp4, tmp3 where ps_partkey = l_partkey " +
        "and ps_suppkey = l_suppkey and ps_availqty > sum_quantity ) select s_name, s_address " +
        s"from ${ tableNames.get("supplier").get} where s_suppkey IN (select ps_suppkey from tmp5) order by s_name"
      )
    )
  }

  // Run all queries for the specified table
  private def time(code: => Unit): Double = {
    val start = System.currentTimeMillis()
    code
    // return time in second
    (System.currentTimeMillis() - start).toDouble / 1000
  }

}
