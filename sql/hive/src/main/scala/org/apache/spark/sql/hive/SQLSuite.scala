package org.apache.spark.sql.hive

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.{SparkConf, SparkContext}

object SQLSuite {

  val master = "ec2-52-0-52-60.compute-1.amazonaws.com"
  val spark = s"spark://$master:7077"
  val hdfs = s"hdfs://$master:9010"

  val sparkConf = new SparkConf()
    .set("spark.sql.test", "")
    .set("spark.executor.memory", "48g")
  val sparkContext = new SparkContext(spark, "SQLSuite", sparkConf)

  val hiveContext = new HiveContext(sparkContext)

  val hiveConf = Seq(
    SQLConf.SHUFFLE_PARTITIONS -> "40",
    SQLConf.CODEGEN_ENABLED -> "false"
  )
  hiveConf.foreach { case (key, value) =>
    hiveContext.setConf(key, value)
  }

  var debug = false

  def main(args: Array[String]): Unit = {
    args(0) match {
      case "create" => create(args(1))
      case "drop" => drop()
      case "show" => show()
      case "test" => test()
      case queryName =>
        hiveRun(s"Q$queryName")
    }
  }

  def hiveRun(queryName: String): Unit = {
    val query = hiveContext.sql(queries(queryName))
    printPlan(query)

    printResult(query)
  }

  def printResult(query: DataFrame): Unit = {
    query.queryExecution.executedPlan // force to initialize the query plan

    var rows: Array[Row] = null
    benchmark {
      rows = query.collect()
    }

    println(s"==============Result===============")
    rows.foreach(row => println(row.mkString(", ")))
    println("====================================")
  }

  def printPlan(query: DataFrame): Unit = {
    if (debug) {
      println("=============Plan=================")
      println(query.queryExecution.executedPlan)
      println("==================================")
    }
  }

  def create(tpch: String): Unit = {
    val ddls = Seq(
      hiveContext.sql(
        s"""
          |create external table lineitem (
          | L_ORDERKEY INT,
          | L_PARTKEY INT,
          | L_SUPPKEY INT,
          | L_LINENUMBER INT,
          | L_QUANTITY DOUBLE,
          | L_EXTENDEDPRICE DOUBLE,
          | L_DISCOUNT DOUBLE,
          | L_TAX DOUBLE,
          | L_RETURNFLAG STRING,
          | L_LINESTATUS STRING,
          | L_SHIPDATE STRING,
          | L_COMMITDATE STRING,
          | L_RECEIPTDATE STRING,
          | L_SHIPINSTRUCT STRING,
          | L_SHIPMODE STRING,
          | L_COMMENT STRING
          |)
          |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED
          |AS TEXTFILE LOCATION '$hdfs/$tpch/lineitem'
        """.stripMargin),
      hiveContext.sql(
        s"""
          |create external table orders (
          | O_ORDERKEY INT,
          | O_CUSTKEY INT,
          | O_ORDERSTATUS STRING,
          | O_TOTALPRICE DOUBLE,
          | O_ORDERDATE STRING,
          | O_ORDERPRIORITY STRING,
          | O_CLERK STRING,
          | O_SHIPPRIORITY INT,
          | O_COMMENT STRING
          |)
          |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED
          |AS TEXTFILE LOCATION '$hdfs/$tpch/orders'
        """.stripMargin),
      hiveContext.sql(
        s"""
          |create external table customer (
          | C_CUSTKEY INT,
          | C_NAME STRING,
          | C_ADDRESS STRING,
          | C_NATIONKEY INT,
          | C_PHONE STRING,
          | C_ACCTBAL DOUBLE,
          | C_MKTSEGMENT STRING,
          | C_COMMENT STRING
          |)
          |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED
          |AS TEXTFILE LOCATION '$hdfs/$tpch/customer'
        """.stripMargin),
      hiveContext.sql(
        s"""
          |create external table supplier (
          | S_SUPPKEY INT,
          | S_NAME STRING,
          | S_ADDRESS STRING,
          | S_NATIONKEY INT,
          | S_PHONE STRING,
          | S_ACCTBAL DOUBLE,
          | S_COMMENT STRING
          |)
          |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED
          |AS TEXTFILE LOCATION '$hdfs/$tpch/supplier'
        """.stripMargin),
      hiveContext.sql(
        s"""
          |create external table partsupp (
          | PS_PARTKEY INT,
          | PS_SUPPKEY INT,
          | PS_AVAILQTY INT,
          | PS_SUPPLYCOST DOUBLE,
          | PS_COMMENT STRING
          |)
          |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED
          |AS TEXTFILE LOCATION '$hdfs/$tpch/partsupp'
        """.stripMargin),
      hiveContext.sql(
        s"""
          |create external table part (
          | P_PARTKEY INT,
          | P_NAME STRING,
          | P_MFGR STRING,
          | P_BRAND STRING,
          | P_TYPE STRING,
          | P_SIZE INT,
          | P_CONTAINER STRING,
          | P_RETAILPRICE DOUBLE,
          | P_COMMENT STRING
          |)
          |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED
          |AS TEXTFILE LOCATION '$hdfs/$tpch/part'
        """.stripMargin),
      hiveContext.sql(
        s"""
          |create external table nation (
          | N_NATIONKEY INT,
          | N_NAME STRING,
          | N_REGIONKEY INT,
          | N_COMMENT STRING
          |)
          |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED
          |AS TEXTFILE LOCATION '$hdfs/$tpch/nation'
        """.stripMargin),
      hiveContext.sql(
        s"""
          |create external table region (
          | R_REGIONKEY INT,
          | R_NAME STRING,
          | R_COMMENT STRING
          |)
          |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED
          |AS TEXTFILE LOCATION '$hdfs/$tpch/region'
        """.stripMargin)
    )

    ddls.foreach(_.collect())
  }

  def drop(): Unit = {
    val tables = Seq("lineitem", "orders", "customer", "supplier", "partsupp", "part", "nation", "region")
    val queries = tables.map(table => hiveContext.sql(s"drop table $table"))
    queries.map(_.collect())
  }

  def show(): Unit = {
    val tables = hiveContext.sql("show tables").collect()
    println("=====================")
    tables.foreach(println(_))
    println("=====================")
  }

  def test(): Unit = {
    val tables = Seq("lineitem", "orders", "customer", "supplier", "partsupp", "part", "nation", "region")
    val queries = tables.map(table => hiveContext.sql(s"select count(*) from $table"))
    val results = queries.map(_.collect())

    println("====================")
    tables.zip(results).foreach {
      case (table, result) => println(s"# of $table = ${result.mkString}")
    }
    println("====================")
  }

  val queries = Map(
    "Q1" -> """
              |SELECT l_returnflag
              |	,l_linestatus
              |	,sum(l_quantity) AS sum_qty
              |	,sum(l_extendedprice) AS sum_base_price
              |	,sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price
              |	,sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge
              |	,avg(l_quantity) AS avg_qty
              |	,avg(l_extendedprice) AS avg_price
              |	,avg(l_discount) AS avg_disc
              |	,count(*) AS count_order
              |FROM lineitem
              |WHERE l_shipdate <= '1998-09-01'
              |GROUP BY l_returnflag
              |	,l_linestatus
            """.stripMargin,
    "Q3" -> """
              |SELECT o_orderdate
              |	,o_shippriority
              |	,sum(l_extendedprice * (1 - l_discount)) AS revenue
              |FROM customer
              |	,orders
              |	,lineitem
              |WHERE c_mktsegment = 'BUILDING'
              |	AND c_custkey = o_custkey
              |	AND l_orderkey = o_orderkey
              |	AND o_orderdate < '1995-07-01'
              |	AND o_orderdate > '1994-01-01'
              |	AND l_shipdate > '1994-01-01'
              |GROUP BY o_orderdate
              |	,o_shippriority
            """.stripMargin,
    "Q5" -> """
              |SELECT n_name
              |	,sum(l_extendedprice * (1 - l_discount)) AS revenue
              |FROM customer
              |	,orders
              |	,lineitem
              |	,supplier
              |	,nation
              |	,region
              |WHERE c_custkey = o_custkey
              |	AND l_orderkey = o_orderkey
              |	AND l_suppkey = s_suppkey
              |	AND c_nationkey = s_nationkey
              |	AND s_nationkey = n_nationkey
              |	AND n_regionkey = r_regionkey
              |	AND r_name = 'AMERICA'
              |	AND o_orderdate >= '1995-01-01'
              |	AND o_orderdate < '1996-01-01'
              |GROUP BY n_name
            """.stripMargin,
    "Q6" -> """
              |SELECT sum(l_extendedprice * l_discount) AS revenues
              |FROM lineitem
              |WHERE l_shipdate >= '1996-01-01'
              |	AND l_shipdate < '1997-01-01'
              |	AND l_discount BETWEEN 0.06 AND 0.08
              |	AND l_quantity < 24
            """.stripMargin,
    "Q7" -> """
              |SELECT supp_nation
              |	,cust_nation
              |	,l_year
              |	,sum(volume) AS revenue
              |FROM (
              |	SELECT n1.n_name AS supp_nation
              |		,n2.n_name AS cust_nation
              |		,substring(l_shipdate, 1, 4) AS l_year
              |		,l_extendedprice * (1 - l_discount) AS volume
              |	FROM supplier
              |		,lineitem
              |		,orders
              |		,customer
              |		,nation n1
              |		,nation n2
              |	WHERE s_suppkey = l_suppkey
              |		AND o_orderkey = l_orderkey
              |		AND c_custkey = o_custkey
              |		AND s_nationkey = n1.n_nationkey
              |		AND c_nationkey = n2.n_nationkey
              |		AND (
              |			(
              |				n1.n_name = 'VIETNAM'
              |				AND n2.n_name = 'KENYA'
              |				)
              |			OR (
              |				n1.n_name = 'KENYA'
              |				AND n2.n_name = 'VIETNAM'
              |				)
              |			)
              |		AND l_shipdate BETWEEN '1995-01-01'
              |			AND '1996-12-31'
              |	) AS shipping
              |GROUP BY supp_nation
              |	,cust_nation
              |	,l_year
            """.stripMargin,
    "Q8" -> """
              |SELECT o_year
              |	,sum(CASE
              |			WHEN nation = 'JAPAN'
              |				THEN volume
              |			ELSE 0
              |			END) / sum(volume) AS mkt_share
              |FROM (
              |	SELECT substring(o_orderdate, 1, 4) AS o_year
              |		,l_extendedprice * (1 - l_discount) AS volume
              |		,n2.n_name AS nation
              |	FROM lineitem
              |   ,part
              |		,supplier
              |		,orders
              |		,customer
              |		,nation n1
              |		,nation n2
              |		,region
              |	WHERE p_partkey = l_partkey
              |		AND s_suppkey = l_suppkey
              |		AND l_orderkey = o_orderkey
              |		AND o_custkey = c_custkey
              |		AND c_nationkey = n1.n_nationkey
              |		AND n1.n_regionkey = r_regionkey
              |		AND r_name = 'ASIA'
              |		AND s_nationkey = n2.n_nationkey
              |		AND o_orderdate BETWEEN '1995-01-01' AND '1996-12-31'
              |		AND p_type = 'LARGE POLISHED BRASS'
              |	) AS all_nations
              |GROUP BY o_year
            """.stripMargin,
    "Q9" -> """
              |SELECT nation
              |	,o_year
              |	,sum(amount) AS sum_profit
              |FROM (
              |	SELECT n_name AS nation
              |		,substring(o_orderdate, 1, 4) AS o_year
              |		,l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
              |	FROM lineitem
              |   ,part
              |		,supplier
              |		,partsupp
              |		,orders
              |		,nation
              |	WHERE s_suppkey = l_suppkey
              |		AND ps_suppkey = l_suppkey
              |		AND ps_partkey = l_partkey
              |		AND p_partkey = l_partkey
              |		AND o_orderkey = l_orderkey
              |		AND s_nationkey = n_nationkey
              |		AND p_name LIKE '%ghost%'
              |	) AS profit
              |GROUP BY nation
              |	,o_year
            """.stripMargin,
    "Q10" -> """
               |SELECT n_name
               |	,sum(l_extendedprice * (1 - l_discount)) AS revenue
               |FROM customer
               |	,orders
               |	,lineitem
               |	,nation
               |WHERE c_custkey = o_custkey
               |	AND l_orderkey = o_orderkey
               |	AND o_orderdate >= '1994-10-01'
               |	AND o_orderdate < '1995-01-01'
               |	AND l_returnflag = 'R'
               |	AND c_nationkey = n_nationkey
               |GROUP BY n_name
             """.stripMargin,
    "Q11" -> """
               |SELECT n_nationkey
               |	,value
               |FROM (
               |	SELECT 0 AS KEY
               |		,n_nationkey
               |		,sum(ps_supplycost * ps_availqty) AS value
               |	FROM partsupp
               |		,supplier
               |		,nation
               |	WHERE ps_suppkey = s_suppkey
               |		AND s_nationkey = n_nationkey
               |	GROUP BY n_nationkey
               |	) AS A
               |	,(
               |		SELECT 0 AS KEY
               |			,sum(ps_supplycost * ps_availqty) * 0.00002 AS threshold
               |		FROM partsupp
               |			,supplier
               |			,nation
               |		WHERE ps_suppkey = s_suppkey
               |			AND s_nationkey = n_nationkey
               |		) AS B
               |WHERE A.KEY = B.KEY
               |	AND value > threshold
             """.stripMargin,
    "Q12" -> """
               |SELECT l_shipmode
               |	,sum(CASE
               |			WHEN o_orderpriority = '1-URGENT'
               |				OR o_orderpriority = '2-HIGH'
               |				THEN 1
               |			ELSE 0
               |			END) AS high_line_count
               |	,sum(CASE
               |			WHEN o_orderpriority <> '1-URGENT'
               |				AND o_orderpriority <> '2-HIGH'
               |				THEN 1
               |			ELSE 0
               |			END) AS low_line_count
               |FROM orders
               |	,lineitem
               |WHERE o_orderkey = l_orderkey
               |	AND l_shipmode IN (
               |		'RAIL'
               |		,'MAIL'
               |		)
               |	AND l_commitdate < l_receiptdate
               |	AND l_shipdate < l_commitdate
               |	AND l_receiptdate >= '1993-01-01'
               |	AND l_receiptdate < '1994-01-01'
               |GROUP BY l_shipmode
             """.stripMargin,
    "Q14" -> """
               |SELECT 100.00 * sum(CASE
               |			WHEN p_type LIKE 'PROMO%'
               |				THEN l_extendedprice * (1 - l_discount)
               |			ELSE 0
               |			END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
               |FROM lineitem
               |	,part
               |WHERE l_partkey = p_partkey
               |	AND l_shipdate >= '1996-12-01'
               |	AND l_shipdate < '1997-01-01'
             """.stripMargin,
    "Q16" -> """
               |SELECT p_type
               |	,p_size
               |	,count(ps_suppkey) AS supplier_cnt
               |FROM (
               |	SELECT p_brand
               |		,p_type
               |		,p_size
               |		,ps_suppkey
               |	FROM partsupp
               |		,part
               |	WHERE p_partkey = ps_partkey
               |		AND p_brand <> 'Brand#43'
               |		AND p_type NOT LIKE 'STANDARD BURNISHED%'
               |		AND p_size IN (
               |			22
               |			,7
               |			,8
               |			,35
               |			,33
               |			,11
               |			,31
               |			,39
               |			)
               |	) A
               |JOIN (
               |	SELECT s_suppkey
               |	FROM supplier
               |	WHERE s_comment NOT LIKE '%Customer%Complaints%'
               | GROUP BY s_suppkey
               |	) B ON ps_suppkey = s_suppkey
               |GROUP BY p_brand
               |	,p_type
               |	,p_size
             """.stripMargin,
    "Q17" -> """
               |SELECT sum(l_extendedprice) / 7.0 AS avg_yearly
               |FROM (
               |	SELECT p_mfgr
               |		,l_quantity
               |		,l_extendedprice
               |	FROM lineitem
               |		,part
               |	WHERE p_partkey = l_partkey
               |		AND p_brand = 'Brand#42'
               |		AND p_container = 'JUMBO BOX'
               |	) AS A
               |	,(
               |		SELECT p_mfgr
               |			,0.2 * avg(l_quantity) AS threshold
               |		FROM lineitem
               |			,part
               |		WHERE l_partkey = p_partkey
               |		GROUP BY p_mfgr
               |		) AS B
               |WHERE A.p_mfgr = B.p_mfgr
               |	AND l_quantity < threshold
             """.stripMargin,
    "Q18" -> """
               |SELECT c_nationkey
               |	,sum(l_quantity)
               |FROM (
               |	SELECT c_nationkey
               |		,l_quantity
               |		,o_orderpriority
               |	FROM customer
               |		,orders
               |		,lineitem
               |	WHERE c_custkey = o_custkey
               |		AND o_orderkey = l_orderkey
               |	) A
               |JOIN (
               |SELECT o_orderpriority
               |FROM (
               |	SELECT o_orderpriority, sum(l_quantity) AS tot_qty
               |	FROM orders
               |		,lineitem
               |	WHERE o_orderkey = l_orderkey
               |	GROUP BY o_orderpriority
               | ) B
               |WHERE tot_qty > 3050250000
               |	) C ON (A.o_orderpriority = C.o_orderpriority)
               |GROUP BY c_nationkey
             """.stripMargin,
    "Q19" -> """
               |SELECT sum(l_extendedprice * (1 - l_discount)) AS revenue
               |FROM lineitem
               |	,part
               |WHERE p_partkey = l_partkey
               |	AND l_shipmode IN (
               |		'AIR'
               |		,'AIR REG'
               |		)
               |	AND l_shipinstruct = 'DELIVER IN PERSON'
               |	AND (
               |		(
               |			p_brand = 'Brand#45'
               |			AND p_container IN (
               |				'SM CASE'
               |				,'SM BOX'
               |				,'SM PACK'
               |				,'SM PKG'
               |				)
               |			AND l_quantity >= 7
               |			AND l_quantity <= 7 + 10
               |			AND p_size BETWEEN 1 AND 50
               |			)
               |		OR (
               |			p_brand = 'Brand#51'
               |			AND p_container IN (
               |				'MED BAG'
               |				,'MED BOX'
               |				,'MED PKG'
               |				,'MED PACK'
               |				)
               |			AND l_quantity >= 20
               |			AND l_quantity <= 20 + 10
               |			AND p_size BETWEEN 1 AND 10
               |			)
               |		OR (
               |			p_brand = 'Brand#51'
               |			AND p_container IN (
               |				'LG CASE'
               |				,'LG BOX'
               |				,'LG PACK'
               |				,'LG PKG'
               |				)
               |			AND l_quantity >= 28
               |			AND l_quantity <= 28 + 10
               |			AND p_size BETWEEN 1 AND 15
               |			)
               |		)
             """.stripMargin,
    "Q20" -> """
               |SELECT s_name
               |	,s_address
               |FROM (
               |	SELECT s_name
               |		,s_address
               |		,s_suppkey
               |	FROM supplier
               |		,nation
               |	WHERE s_nationkey = n_nationkey
               |		AND n_name = 'ETHIOPIA'
               |	) A
               |JOIN (
               |	SELECT ps_suppkey
               |	FROM partsupp
               |	JOIN (
               |		SELECT p_partkey
               |		FROM part
               |		WHERE p_name LIKE 'cornsilk%'
               |   GROUP BY p_partkey
               |		) C ON (ps_partkey = p_partkey)
               |	JOIN (
               |		SELECT s_suppkey
               |			,qty
               |		FROM (
               |			SELECT s_nationkey AS nationkey
               |				,sum(l_quantity) * 0.0001 AS qty
               |			FROM lineitem
               |				,supplier
               |			WHERE l_suppkey = s_suppkey
               |				AND l_shipdate >= '1996-01-01'
               |				AND l_shipdate < '1997-01-01'
               |			GROUP BY s_nationkey
               |			) lqty
               |			,supplier
               |		WHERE s_nationkey = nationkey
               |		) D ON (ps_suppkey = s_suppkey)
               |	WHERE ps_availqty > qty
               | GROUP BY ps_suppkey
               |	) B ON (s_suppkey = ps_suppkey)
             """.stripMargin,
    "Q22" -> """
               |SELECT cntrycode
               |	,count(*) AS numcust
               |	,sum(c_acctbal) AS totacctbal
               |FROM (
               |	SELECT 0 AS KEY
               |		,substring(c_phone, 1, 2) AS cntrycode
               |		,c_acctbal
               |	FROM customer
               |	WHERE substring(c_phone, 1, 2) IN (
               |			'26'
               |			,'29'
               |			,'31'
               |			,'28'
               |			,'30'
               |			,'25'
               |			,'40'
               |			)
               |	) AS A
               |	,(
               |		SELECT 0 AS KEY
               |			,avg(c_acctbal) AS threshold
               |		FROM customer
               |		WHERE c_acctbal > 0.00
               |			AND substring(c_phone, 1, 2) IN (
               |				'26'
               |				,'29'
               |				,'31'
               |				,'28'
               |				,'30'
               |				,'25'
               |				,'40'
               |				)
               |		) AS B
               |WHERE A.KEY = B.KEY
               |	AND c_acctbal > threshold
               |GROUP BY cntrycode
             """.stripMargin,
    "Qplus" -> """
                |SELECT l_returnflag
                |	,l_linestatus
                |	,sum(l_quantity + 1)
                |	,sum(l_quantity + 2)
                |	,sum(l_quantity + 3)
                |	,sum(l_quantity + 4)
                |	,sum(l_quantity + 5)
                |	,sum(l_quantity + 6)
                |	,sum(l_quantity + 7)
                |	,sum(l_quantity + 8)
                |	,sum(l_quantity + 9)
                |	,sum(l_quantity + 10)
                |	,sum(l_quantity + 11)
                |	,sum(l_quantity + 12)
                |	,sum(l_quantity + 13)
                |	,sum(l_quantity + 14)
                |	,sum(l_quantity + 15)
                |	,sum(l_quantity + 16)
                |	,sum(l_quantity + 17)
                |	,sum(l_quantity + 18)
                |	,sum(l_quantity + 19)
                |	,sum(l_quantity + 20)
                |	,sum(l_quantity + 21)
                |	,sum(l_quantity + 22)
                |	,sum(l_quantity + 23)
                |	,sum(l_quantity + 24)
                |	,sum(l_quantity + 25)
                |	,sum(l_quantity + 26)
                |	,sum(l_quantity + 27)
                |	,sum(l_quantity + 28)
                |	,sum(l_quantity + 29)
                |	,sum(l_quantity + 30)
                |	,sum(l_quantity + 31)
                |	,sum(l_quantity + 32)
                |	,sum(l_quantity + 33)
                |	,sum(l_quantity + 34)
                |	,sum(l_quantity + 35)
                |	,sum(l_quantity + 36)
                |	,sum(l_quantity + 37)
                |	,sum(l_quantity + 38)
                |	,sum(l_quantity + 39)
                |	,sum(l_quantity + 40)
                |	,sum(l_quantity + 41)
                |	,sum(l_quantity + 42)
                |	,sum(l_quantity + 43)
                |	,sum(l_quantity + 44)
                |	,sum(l_quantity + 45)
                |	,sum(l_quantity + 46)
                |	,sum(l_quantity + 47)
                |	,sum(l_quantity + 48)
                |	,sum(l_quantity + 49)
                |	,sum(l_quantity + 50)
                |	,sum(l_quantity + 51)
                |	,sum(l_quantity + 52)
                |	,sum(l_quantity + 53)
                |	,sum(l_quantity + 54)
                |	,sum(l_quantity + 55)
                |	,sum(l_quantity + 56)
                |	,sum(l_quantity + 57)
                |	,sum(l_quantity + 58)
                |	,sum(l_quantity + 59)
                |	,sum(l_quantity + 60)
                |	,sum(l_quantity + 61)
                |	,sum(l_quantity + 62)
                |	,sum(l_quantity + 63)
                |	,sum(l_quantity + 64)
                |	,sum(l_quantity + 65)
                |	,sum(l_quantity + 66)
                |	,sum(l_quantity + 67)
                |	,sum(l_quantity + 68)
                |	,sum(l_quantity + 69)
                |	,sum(l_quantity + 70)
                |	,sum(l_quantity + 71)
                |	,sum(l_quantity + 72)
                |	,sum(l_quantity + 73)
                |	,sum(l_quantity + 74)
                |	,sum(l_quantity + 75)
                |	,sum(l_quantity + 76)
                |	,sum(l_quantity + 77)
                |	,sum(l_quantity + 78)
                |	,sum(l_quantity + 79)
                |	,sum(l_quantity + 80)
                |	,sum(l_quantity + 81)
                |	,sum(l_quantity + 82)
                |	,sum(l_quantity + 83)
                |	,sum(l_quantity + 84)
                |	,sum(l_quantity + 85)
                |	,sum(l_quantity + 86)
                |	,sum(l_quantity + 87)
                |	,sum(l_quantity + 88)
                |	,sum(l_quantity + 89)
                |	,sum(l_quantity + 90)
                |	,sum(l_quantity + 91)
                |	,sum(l_quantity + 92)
                |	,sum(l_quantity + 93)
                |	,sum(l_quantity + 94)
                |	,sum(l_quantity + 95)
                |	,sum(l_quantity + 96)
                |	,sum(l_quantity + 97)
                |	,sum(l_quantity + 98)
                |	,sum(l_quantity + 99)
                |	,sum(l_quantity + 100)
                |FROM lineitem
                |WHERE l_shipdate <= '1998-09-01'
                |GROUP BY l_returnflag
                |	,l_linestatus
              """.stripMargin,
    "Qtimes" -> """
                |SELECT l_returnflag
                |	,l_linestatus
                |	,sum(l_quantity * cnt1)
                |	,sum(l_quantity * cnt2)
                |	,sum(l_quantity * cnt3)
                |	,sum(l_quantity * cnt4)
                |	,sum(l_quantity * cnt5)
                |	,sum(l_quantity * cnt6)
                |	,sum(l_quantity * cnt7)
                |	,sum(l_quantity * cnt8)
                |	,sum(l_quantity * cnt9)
                |	,sum(l_quantity * cnt10)
                |	,sum(l_quantity * cnt11)
                |	,sum(l_quantity * cnt12)
                |	,sum(l_quantity * cnt13)
                |	,sum(l_quantity * cnt14)
                |	,sum(l_quantity * cnt15)
                |	,sum(l_quantity * cnt16)
                |	,sum(l_quantity * cnt17)
                |	,sum(l_quantity * cnt18)
                |	,sum(l_quantity * cnt19)
                |	,sum(l_quantity * cnt20)
                |	,sum(l_quantity * cnt21)
                |	,sum(l_quantity * cnt22)
                |	,sum(l_quantity * cnt23)
                |	,sum(l_quantity * cnt24)
                |	,sum(l_quantity * cnt25)
                |	,sum(l_quantity * cnt26)
                |	,sum(l_quantity * cnt27)
                |	,sum(l_quantity * cnt28)
                |	,sum(l_quantity * cnt29)
                |	,sum(l_quantity * cnt30)
                |	,sum(l_quantity * cnt31)
                |	,sum(l_quantity * cnt32)
                |	,sum(l_quantity * cnt33)
                |	,sum(l_quantity * cnt34)
                |	,sum(l_quantity * cnt35)
                |	,sum(l_quantity * cnt36)
                |	,sum(l_quantity * cnt37)
                |	,sum(l_quantity * cnt38)
                |	,sum(l_quantity * cnt39)
                |	,sum(l_quantity * cnt40)
                |	,sum(l_quantity * cnt41)
                |	,sum(l_quantity * cnt42)
                |	,sum(l_quantity * cnt43)
                |	,sum(l_quantity * cnt44)
                |	,sum(l_quantity * cnt45)
                |	,sum(l_quantity * cnt46)
                |	,sum(l_quantity * cnt47)
                |	,sum(l_quantity * cnt48)
                |	,sum(l_quantity * cnt49)
                |	,sum(l_quantity * cnt50)
                |	,sum(l_quantity * cnt51)
                |	,sum(l_quantity * cnt52)
                |	,sum(l_quantity * cnt53)
                |	,sum(l_quantity * cnt54)
                |	,sum(l_quantity * cnt55)
                |	,sum(l_quantity * cnt56)
                |	,sum(l_quantity * cnt57)
                |	,sum(l_quantity * cnt58)
                |	,sum(l_quantity * cnt59)
                |	,sum(l_quantity * cnt60)
                |	,sum(l_quantity * cnt61)
                |	,sum(l_quantity * cnt62)
                |	,sum(l_quantity * cnt63)
                |	,sum(l_quantity * cnt64)
                |	,sum(l_quantity * cnt65)
                |	,sum(l_quantity * cnt66)
                |	,sum(l_quantity * cnt67)
                |	,sum(l_quantity * cnt68)
                |	,sum(l_quantity * cnt69)
                |	,sum(l_quantity * cnt70)
                |	,sum(l_quantity * cnt71)
                |	,sum(l_quantity * cnt72)
                |	,sum(l_quantity * cnt73)
                |	,sum(l_quantity * cnt74)
                |	,sum(l_quantity * cnt75)
                |	,sum(l_quantity * cnt76)
                |	,sum(l_quantity * cnt77)
                |	,sum(l_quantity * cnt78)
                |	,sum(l_quantity * cnt79)
                |	,sum(l_quantity * cnt80)
                |	,sum(l_quantity * cnt81)
                |	,sum(l_quantity * cnt82)
                |	,sum(l_quantity * cnt83)
                |	,sum(l_quantity * cnt84)
                |	,sum(l_quantity * cnt85)
                |	,sum(l_quantity * cnt86)
                |	,sum(l_quantity * cnt87)
                |	,sum(l_quantity * cnt88)
                |	,sum(l_quantity * cnt89)
                |	,sum(l_quantity * cnt90)
                |	,sum(l_quantity * cnt91)
                |	,sum(l_quantity * cnt92)
                |	,sum(l_quantity * cnt93)
                |	,sum(l_quantity * cnt94)
                |	,sum(l_quantity * cnt95)
                |	,sum(l_quantity * cnt96)
                |	,sum(l_quantity * cnt97)
                |	,sum(l_quantity * cnt98)
                |	,sum(l_quantity * cnt99)
                |	,sum(l_quantity * cnt100)
                |FROM (
                |	SELECT l_returnflag
                |		,l_linestatus
                |		,l_quantity
                |		,1 AS cnt1
                |		,2 AS cnt2
                |		,3 AS cnt3
                |		,4 AS cnt4
                |		,5 AS cnt5
                |		,6 AS cnt6
                |		,7 AS cnt7
                |		,8 AS cnt8
                |		,9 AS cnt9
                |		,10 AS cnt10
                |		,11 AS cnt11
                |		,12 AS cnt12
                |		,13 AS cnt13
                |		,14 AS cnt14
                |		,15 AS cnt15
                |		,16 AS cnt16
                |		,17 AS cnt17
                |		,18 AS cnt18
                |		,19 AS cnt19
                |		,20 AS cnt20
                |		,21 AS cnt21
                |		,22 AS cnt22
                |		,23 AS cnt23
                |		,24 AS cnt24
                |		,25 AS cnt25
                |		,26 AS cnt26
                |		,27 AS cnt27
                |		,28 AS cnt28
                |		,29 AS cnt29
                |		,30 AS cnt30
                |		,31 AS cnt31
                |		,32 AS cnt32
                |		,33 AS cnt33
                |		,34 AS cnt34
                |		,35 AS cnt35
                |		,36 AS cnt36
                |		,37 AS cnt37
                |		,38 AS cnt38
                |		,39 AS cnt39
                |		,40 AS cnt40
                |		,41 AS cnt41
                |		,42 AS cnt42
                |		,43 AS cnt43
                |		,44 AS cnt44
                |		,45 AS cnt45
                |		,46 AS cnt46
                |		,47 AS cnt47
                |		,48 AS cnt48
                |		,49 AS cnt49
                |		,50 AS cnt50
                |		,51 AS cnt51
                |		,52 AS cnt52
                |		,53 AS cnt53
                |		,54 AS cnt54
                |		,55 AS cnt55
                |		,56 AS cnt56
                |		,57 AS cnt57
                |		,58 AS cnt58
                |		,59 AS cnt59
                |		,60 AS cnt60
                |		,61 AS cnt61
                |		,62 AS cnt62
                |		,63 AS cnt63
                |		,64 AS cnt64
                |		,65 AS cnt65
                |		,66 AS cnt66
                |		,67 AS cnt67
                |		,68 AS cnt68
                |		,69 AS cnt69
                |		,70 AS cnt70
                |		,71 AS cnt71
                |		,72 AS cnt72
                |		,73 AS cnt73
                |		,74 AS cnt74
                |		,75 AS cnt75
                |		,76 AS cnt76
                |		,77 AS cnt77
                |		,78 AS cnt78
                |		,79 AS cnt79
                |		,80 AS cnt80
                |		,81 AS cnt81
                |		,82 AS cnt82
                |		,83 AS cnt83
                |		,84 AS cnt84
                |		,85 AS cnt85
                |		,86 AS cnt86
                |		,87 AS cnt87
                |		,88 AS cnt88
                |		,89 AS cnt89
                |		,90 AS cnt90
                |		,91 AS cnt91
                |		,92 AS cnt92
                |		,93 AS cnt93
                |		,94 AS cnt94
                |		,95 AS cnt95
                |		,96 AS cnt96
                |		,97 AS cnt97
                |		,98 AS cnt98
                |		,99 AS cnt99
                |		,100 AS cnt100
                |	FROM lineitem
                |	WHERE l_shipdate <= '1998-09-01'
                |	) AS A
                |GROUP BY l_returnflag
                |	,l_linestatus
              """.stripMargin,
    "Qdiv" -> """
                |SELECT l_returnflag
                |	,l_linestatus
                |	,sum(l_quantity / cnt1)
                |	,sum(l_quantity / cnt2)
                |	,sum(l_quantity / cnt3)
                |	,sum(l_quantity / cnt4)
                |	,sum(l_quantity / cnt5)
                |	,sum(l_quantity / cnt6)
                |	,sum(l_quantity / cnt7)
                |	,sum(l_quantity / cnt8)
                |	,sum(l_quantity / cnt9)
                |	,sum(l_quantity / cnt10)
                |	,sum(l_quantity / cnt11)
                |	,sum(l_quantity / cnt12)
                |	,sum(l_quantity / cnt13)
                |	,sum(l_quantity / cnt14)
                |	,sum(l_quantity / cnt15)
                |	,sum(l_quantity / cnt16)
                |	,sum(l_quantity / cnt17)
                |	,sum(l_quantity / cnt18)
                |	,sum(l_quantity / cnt19)
                |	,sum(l_quantity / cnt20)
                |	,sum(l_quantity / cnt21)
                |	,sum(l_quantity / cnt22)
                |	,sum(l_quantity / cnt23)
                |	,sum(l_quantity / cnt24)
                |	,sum(l_quantity / cnt25)
                |	,sum(l_quantity / cnt26)
                |	,sum(l_quantity / cnt27)
                |	,sum(l_quantity / cnt28)
                |	,sum(l_quantity / cnt29)
                |	,sum(l_quantity / cnt30)
                |	,sum(l_quantity / cnt31)
                |	,sum(l_quantity / cnt32)
                |	,sum(l_quantity / cnt33)
                |	,sum(l_quantity / cnt34)
                |	,sum(l_quantity / cnt35)
                |	,sum(l_quantity / cnt36)
                |	,sum(l_quantity / cnt37)
                |	,sum(l_quantity / cnt38)
                |	,sum(l_quantity / cnt39)
                |	,sum(l_quantity / cnt40)
                |	,sum(l_quantity / cnt41)
                |	,sum(l_quantity / cnt42)
                |	,sum(l_quantity / cnt43)
                |	,sum(l_quantity / cnt44)
                |	,sum(l_quantity / cnt45)
                |	,sum(l_quantity / cnt46)
                |	,sum(l_quantity / cnt47)
                |	,sum(l_quantity / cnt48)
                |	,sum(l_quantity / cnt49)
                |	,sum(l_quantity / cnt50)
                |	,sum(l_quantity / cnt51)
                |	,sum(l_quantity / cnt52)
                |	,sum(l_quantity / cnt53)
                |	,sum(l_quantity / cnt54)
                |	,sum(l_quantity / cnt55)
                |	,sum(l_quantity / cnt56)
                |	,sum(l_quantity / cnt57)
                |	,sum(l_quantity / cnt58)
                |	,sum(l_quantity / cnt59)
                |	,sum(l_quantity / cnt60)
                |	,sum(l_quantity / cnt61)
                |	,sum(l_quantity / cnt62)
                |	,sum(l_quantity / cnt63)
                |	,sum(l_quantity / cnt64)
                |	,sum(l_quantity / cnt65)
                |	,sum(l_quantity / cnt66)
                |	,sum(l_quantity / cnt67)
                |	,sum(l_quantity / cnt68)
                |	,sum(l_quantity / cnt69)
                |	,sum(l_quantity / cnt70)
                |	,sum(l_quantity / cnt71)
                |	,sum(l_quantity / cnt72)
                |	,sum(l_quantity / cnt73)
                |	,sum(l_quantity / cnt74)
                |	,sum(l_quantity / cnt75)
                |	,sum(l_quantity / cnt76)
                |	,sum(l_quantity / cnt77)
                |	,sum(l_quantity / cnt78)
                |	,sum(l_quantity / cnt79)
                |	,sum(l_quantity / cnt80)
                |	,sum(l_quantity / cnt81)
                |	,sum(l_quantity / cnt82)
                |	,sum(l_quantity / cnt83)
                |	,sum(l_quantity / cnt84)
                |	,sum(l_quantity / cnt85)
                |	,sum(l_quantity / cnt86)
                |	,sum(l_quantity / cnt87)
                |	,sum(l_quantity / cnt88)
                |	,sum(l_quantity / cnt89)
                |	,sum(l_quantity / cnt90)
                |	,sum(l_quantity / cnt91)
                |	,sum(l_quantity / cnt92)
                |	,sum(l_quantity / cnt93)
                |	,sum(l_quantity / cnt94)
                |	,sum(l_quantity / cnt95)
                |	,sum(l_quantity / cnt96)
                |	,sum(l_quantity / cnt97)
                |	,sum(l_quantity / cnt98)
                |	,sum(l_quantity / cnt99)
                |	,sum(l_quantity / cnt100)
                |FROM (
                |	SELECT l_returnflag
                |		,l_linestatus
                |		,l_quantity
                |		,1 AS cnt1
                |		,2 AS cnt2
                |		,3 AS cnt3
                |		,4 AS cnt4
                |		,5 AS cnt5
                |		,6 AS cnt6
                |		,7 AS cnt7
                |		,8 AS cnt8
                |		,9 AS cnt9
                |		,10 AS cnt10
                |		,11 AS cnt11
                |		,12 AS cnt12
                |		,13 AS cnt13
                |		,14 AS cnt14
                |		,15 AS cnt15
                |		,16 AS cnt16
                |		,17 AS cnt17
                |		,18 AS cnt18
                |		,19 AS cnt19
                |		,20 AS cnt20
                |		,21 AS cnt21
                |		,22 AS cnt22
                |		,23 AS cnt23
                |		,24 AS cnt24
                |		,25 AS cnt25
                |		,26 AS cnt26
                |		,27 AS cnt27
                |		,28 AS cnt28
                |		,29 AS cnt29
                |		,30 AS cnt30
                |		,31 AS cnt31
                |		,32 AS cnt32
                |		,33 AS cnt33
                |		,34 AS cnt34
                |		,35 AS cnt35
                |		,36 AS cnt36
                |		,37 AS cnt37
                |		,38 AS cnt38
                |		,39 AS cnt39
                |		,40 AS cnt40
                |		,41 AS cnt41
                |		,42 AS cnt42
                |		,43 AS cnt43
                |		,44 AS cnt44
                |		,45 AS cnt45
                |		,46 AS cnt46
                |		,47 AS cnt47
                |		,48 AS cnt48
                |		,49 AS cnt49
                |		,50 AS cnt50
                |		,51 AS cnt51
                |		,52 AS cnt52
                |		,53 AS cnt53
                |		,54 AS cnt54
                |		,55 AS cnt55
                |		,56 AS cnt56
                |		,57 AS cnt57
                |		,58 AS cnt58
                |		,59 AS cnt59
                |		,60 AS cnt60
                |		,61 AS cnt61
                |		,62 AS cnt62
                |		,63 AS cnt63
                |		,64 AS cnt64
                |		,65 AS cnt65
                |		,66 AS cnt66
                |		,67 AS cnt67
                |		,68 AS cnt68
                |		,69 AS cnt69
                |		,70 AS cnt70
                |		,71 AS cnt71
                |		,72 AS cnt72
                |		,73 AS cnt73
                |		,74 AS cnt74
                |		,75 AS cnt75
                |		,76 AS cnt76
                |		,77 AS cnt77
                |		,78 AS cnt78
                |		,79 AS cnt79
                |		,80 AS cnt80
                |		,81 AS cnt81
                |		,82 AS cnt82
                |		,83 AS cnt83
                |		,84 AS cnt84
                |		,85 AS cnt85
                |		,86 AS cnt86
                |		,87 AS cnt87
                |		,88 AS cnt88
                |		,89 AS cnt89
                |		,90 AS cnt90
                |		,91 AS cnt91
                |		,92 AS cnt92
                |		,93 AS cnt93
                |		,94 AS cnt94
                |		,95 AS cnt95
                |		,96 AS cnt96
                |		,97 AS cnt97
                |		,98 AS cnt98
                |		,99 AS cnt99
                |		,100 AS cnt100
                |	FROM lineitem
                |	WHERE l_shipdate <= '1998-09-01'
                |	) AS A
                |GROUP BY l_returnflag
                |	,l_linestatus
              """.stripMargin,
    "Qquot" -> """
                |SELECT l_returnflag
                |	,l_linestatus
                |	,sum(quantity / cnt1)
                |	,sum(quantity / cnt2)
                |	,sum(quantity / cnt3)
                |	,sum(quantity / cnt4)
                |	,sum(quantity / cnt5)
                |	,sum(quantity / cnt6)
                |	,sum(quantity / cnt7)
                |	,sum(quantity / cnt8)
                |	,sum(quantity / cnt9)
                |	,sum(quantity / cnt10)
                |	,sum(quantity / cnt11)
                |	,sum(quantity / cnt12)
                |	,sum(quantity / cnt13)
                |	,sum(quantity / cnt14)
                |	,sum(quantity / cnt15)
                |	,sum(quantity / cnt16)
                |	,sum(quantity / cnt17)
                |	,sum(quantity / cnt18)
                |	,sum(quantity / cnt19)
                |	,sum(quantity / cnt20)
                |	,sum(quantity / cnt21)
                |	,sum(quantity / cnt22)
                |	,sum(quantity / cnt23)
                |	,sum(quantity / cnt24)
                |	,sum(quantity / cnt25)
                |	,sum(quantity / cnt26)
                |	,sum(quantity / cnt27)
                |	,sum(quantity / cnt28)
                |	,sum(quantity / cnt29)
                |	,sum(quantity / cnt30)
                |	,sum(quantity / cnt31)
                |	,sum(quantity / cnt32)
                |	,sum(quantity / cnt33)
                |	,sum(quantity / cnt34)
                |	,sum(quantity / cnt35)
                |	,sum(quantity / cnt36)
                |	,sum(quantity / cnt37)
                |	,sum(quantity / cnt38)
                |	,sum(quantity / cnt39)
                |	,sum(quantity / cnt40)
                |	,sum(quantity / cnt41)
                |	,sum(quantity / cnt42)
                |	,sum(quantity / cnt43)
                |	,sum(quantity / cnt44)
                |	,sum(quantity / cnt45)
                |	,sum(quantity / cnt46)
                |	,sum(quantity / cnt47)
                |	,sum(quantity / cnt48)
                |	,sum(quantity / cnt49)
                |	,sum(quantity / cnt50)
                |	,sum(quantity / cnt51)
                |	,sum(quantity / cnt52)
                |	,sum(quantity / cnt53)
                |	,sum(quantity / cnt54)
                |	,sum(quantity / cnt55)
                |	,sum(quantity / cnt56)
                |	,sum(quantity / cnt57)
                |	,sum(quantity / cnt58)
                |	,sum(quantity / cnt59)
                |	,sum(quantity / cnt60)
                |	,sum(quantity / cnt61)
                |	,sum(quantity / cnt62)
                |	,sum(quantity / cnt63)
                |	,sum(quantity / cnt64)
                |	,sum(quantity / cnt65)
                |	,sum(quantity / cnt66)
                |	,sum(quantity / cnt67)
                |	,sum(quantity / cnt68)
                |	,sum(quantity / cnt69)
                |	,sum(quantity / cnt70)
                |	,sum(quantity / cnt71)
                |	,sum(quantity / cnt72)
                |	,sum(quantity / cnt73)
                |	,sum(quantity / cnt74)
                |	,sum(quantity / cnt75)
                |	,sum(quantity / cnt76)
                |	,sum(quantity / cnt77)
                |	,sum(quantity / cnt78)
                |	,sum(quantity / cnt79)
                |	,sum(quantity / cnt80)
                |	,sum(quantity / cnt81)
                |	,sum(quantity / cnt82)
                |	,sum(quantity / cnt83)
                |	,sum(quantity / cnt84)
                |	,sum(quantity / cnt85)
                |	,sum(quantity / cnt86)
                |	,sum(quantity / cnt87)
                |	,sum(quantity / cnt88)
                |	,sum(quantity / cnt89)
                |	,sum(quantity / cnt90)
                |	,sum(quantity / cnt91)
                |	,sum(quantity / cnt92)
                |	,sum(quantity / cnt93)
                |	,sum(quantity / cnt94)
                |	,sum(quantity / cnt95)
                |	,sum(quantity / cnt96)
                |	,sum(quantity / cnt97)
                |	,sum(quantity / cnt98)
                |	,sum(quantity / cnt99)
                |	,sum(quantity / cnt100)
                |FROM (
                |	SELECT l_returnflag
                |		,l_linestatus
                |		,CAST(l_quantity AS INT) AS quantity
                |		,1 AS cnt1
                |		,2 AS cnt2
                |		,3 AS cnt3
                |		,4 AS cnt4
                |		,5 AS cnt5
                |		,6 AS cnt6
                |		,7 AS cnt7
                |		,8 AS cnt8
                |		,9 AS cnt9
                |		,10 AS cnt10
                |		,11 AS cnt11
                |		,12 AS cnt12
                |		,13 AS cnt13
                |		,14 AS cnt14
                |		,15 AS cnt15
                |		,16 AS cnt16
                |		,17 AS cnt17
                |		,18 AS cnt18
                |		,19 AS cnt19
                |		,20 AS cnt20
                |		,21 AS cnt21
                |		,22 AS cnt22
                |		,23 AS cnt23
                |		,24 AS cnt24
                |		,25 AS cnt25
                |		,26 AS cnt26
                |		,27 AS cnt27
                |		,28 AS cnt28
                |		,29 AS cnt29
                |		,30 AS cnt30
                |		,31 AS cnt31
                |		,32 AS cnt32
                |		,33 AS cnt33
                |		,34 AS cnt34
                |		,35 AS cnt35
                |		,36 AS cnt36
                |		,37 AS cnt37
                |		,38 AS cnt38
                |		,39 AS cnt39
                |		,40 AS cnt40
                |		,41 AS cnt41
                |		,42 AS cnt42
                |		,43 AS cnt43
                |		,44 AS cnt44
                |		,45 AS cnt45
                |		,46 AS cnt46
                |		,47 AS cnt47
                |		,48 AS cnt48
                |		,49 AS cnt49
                |		,50 AS cnt50
                |		,51 AS cnt51
                |		,52 AS cnt52
                |		,53 AS cnt53
                |		,54 AS cnt54
                |		,55 AS cnt55
                |		,56 AS cnt56
                |		,57 AS cnt57
                |		,58 AS cnt58
                |		,59 AS cnt59
                |		,60 AS cnt60
                |		,61 AS cnt61
                |		,62 AS cnt62
                |		,63 AS cnt63
                |		,64 AS cnt64
                |		,65 AS cnt65
                |		,66 AS cnt66
                |		,67 AS cnt67
                |		,68 AS cnt68
                |		,69 AS cnt69
                |		,70 AS cnt70
                |		,71 AS cnt71
                |		,72 AS cnt72
                |		,73 AS cnt73
                |		,74 AS cnt74
                |		,75 AS cnt75
                |		,76 AS cnt76
                |		,77 AS cnt77
                |		,78 AS cnt78
                |		,79 AS cnt79
                |		,80 AS cnt80
                |		,81 AS cnt81
                |		,82 AS cnt82
                |		,83 AS cnt83
                |		,84 AS cnt84
                |		,85 AS cnt85
                |		,86 AS cnt86
                |		,87 AS cnt87
                |		,88 AS cnt88
                |		,89 AS cnt89
                |		,90 AS cnt90
                |		,91 AS cnt91
                |		,92 AS cnt92
                |		,93 AS cnt93
                |		,94 AS cnt94
                |		,95 AS cnt95
                |		,96 AS cnt96
                |		,97 AS cnt97
                |		,98 AS cnt98
                |		,99 AS cnt99
                |		,100 AS cnt100
                |	FROM lineitem
                |	WHERE l_shipdate <= '1998-09-01'
                |	) AS A
                |GROUP BY l_returnflag
                |	,l_linestatus
              """.stripMargin,
    "Qrem" -> """
                |SELECT l_returnflag
                |	,l_linestatus
                |	,sum(l_quantity % cnt1)
                |	,sum(l_quantity % cnt2)
                |	,sum(l_quantity % cnt3)
                |	,sum(l_quantity % cnt4)
                |	,sum(l_quantity % cnt5)
                |	,sum(l_quantity % cnt6)
                |	,sum(l_quantity % cnt7)
                |	,sum(l_quantity % cnt8)
                |	,sum(l_quantity % cnt9)
                |	,sum(l_quantity % cnt10)
                |	,sum(l_quantity % cnt11)
                |	,sum(l_quantity % cnt12)
                |	,sum(l_quantity % cnt13)
                |	,sum(l_quantity % cnt14)
                |	,sum(l_quantity % cnt15)
                |	,sum(l_quantity % cnt16)
                |	,sum(l_quantity % cnt17)
                |	,sum(l_quantity % cnt18)
                |	,sum(l_quantity % cnt19)
                |	,sum(l_quantity % cnt20)
                |	,sum(l_quantity % cnt21)
                |	,sum(l_quantity % cnt22)
                |	,sum(l_quantity % cnt23)
                |	,sum(l_quantity % cnt24)
                |	,sum(l_quantity % cnt25)
                |	,sum(l_quantity % cnt26)
                |	,sum(l_quantity % cnt27)
                |	,sum(l_quantity % cnt28)
                |	,sum(l_quantity % cnt29)
                |	,sum(l_quantity % cnt30)
                |	,sum(l_quantity % cnt31)
                |	,sum(l_quantity % cnt32)
                |	,sum(l_quantity % cnt33)
                |	,sum(l_quantity % cnt34)
                |	,sum(l_quantity % cnt35)
                |	,sum(l_quantity % cnt36)
                |	,sum(l_quantity % cnt37)
                |	,sum(l_quantity % cnt38)
                |	,sum(l_quantity % cnt39)
                |	,sum(l_quantity % cnt40)
                |	,sum(l_quantity % cnt41)
                |	,sum(l_quantity % cnt42)
                |	,sum(l_quantity % cnt43)
                |	,sum(l_quantity % cnt44)
                |	,sum(l_quantity % cnt45)
                |	,sum(l_quantity % cnt46)
                |	,sum(l_quantity % cnt47)
                |	,sum(l_quantity % cnt48)
                |	,sum(l_quantity % cnt49)
                |	,sum(l_quantity % cnt50)
                |	,sum(l_quantity % cnt51)
                |	,sum(l_quantity % cnt52)
                |	,sum(l_quantity % cnt53)
                |	,sum(l_quantity % cnt54)
                |	,sum(l_quantity % cnt55)
                |	,sum(l_quantity % cnt56)
                |	,sum(l_quantity % cnt57)
                |	,sum(l_quantity % cnt58)
                |	,sum(l_quantity % cnt59)
                |	,sum(l_quantity % cnt60)
                |	,sum(l_quantity % cnt61)
                |	,sum(l_quantity % cnt62)
                |	,sum(l_quantity % cnt63)
                |	,sum(l_quantity % cnt64)
                |	,sum(l_quantity % cnt65)
                |	,sum(l_quantity % cnt66)
                |	,sum(l_quantity % cnt67)
                |	,sum(l_quantity % cnt68)
                |	,sum(l_quantity % cnt69)
                |	,sum(l_quantity % cnt70)
                |	,sum(l_quantity % cnt71)
                |	,sum(l_quantity % cnt72)
                |	,sum(l_quantity % cnt73)
                |	,sum(l_quantity % cnt74)
                |	,sum(l_quantity % cnt75)
                |	,sum(l_quantity % cnt76)
                |	,sum(l_quantity % cnt77)
                |	,sum(l_quantity % cnt78)
                |	,sum(l_quantity % cnt79)
                |	,sum(l_quantity % cnt80)
                |	,sum(l_quantity % cnt81)
                |	,sum(l_quantity % cnt82)
                |	,sum(l_quantity % cnt83)
                |	,sum(l_quantity % cnt84)
                |	,sum(l_quantity % cnt85)
                |	,sum(l_quantity % cnt86)
                |	,sum(l_quantity % cnt87)
                |	,sum(l_quantity % cnt88)
                |	,sum(l_quantity % cnt89)
                |	,sum(l_quantity % cnt90)
                |	,sum(l_quantity % cnt91)
                |	,sum(l_quantity % cnt92)
                |	,sum(l_quantity % cnt93)
                |	,sum(l_quantity % cnt94)
                |	,sum(l_quantity % cnt95)
                |	,sum(l_quantity % cnt96)
                |	,sum(l_quantity % cnt97)
                |	,sum(l_quantity % cnt98)
                |	,sum(l_quantity % cnt99)
                |	,sum(l_quantity % cnt100)
                |FROM (
                |	SELECT l_returnflag
                |		,l_linestatus
                |		,l_quantity
                |		,1 AS cnt1
                |		,2 AS cnt2
                |		,3 AS cnt3
                |		,4 AS cnt4
                |		,5 AS cnt5
                |		,6 AS cnt6
                |		,7 AS cnt7
                |		,8 AS cnt8
                |		,9 AS cnt9
                |		,10 AS cnt10
                |		,11 AS cnt11
                |		,12 AS cnt12
                |		,13 AS cnt13
                |		,14 AS cnt14
                |		,15 AS cnt15
                |		,16 AS cnt16
                |		,17 AS cnt17
                |		,18 AS cnt18
                |		,19 AS cnt19
                |		,20 AS cnt20
                |		,21 AS cnt21
                |		,22 AS cnt22
                |		,23 AS cnt23
                |		,24 AS cnt24
                |		,25 AS cnt25
                |		,26 AS cnt26
                |		,27 AS cnt27
                |		,28 AS cnt28
                |		,29 AS cnt29
                |		,30 AS cnt30
                |		,31 AS cnt31
                |		,32 AS cnt32
                |		,33 AS cnt33
                |		,34 AS cnt34
                |		,35 AS cnt35
                |		,36 AS cnt36
                |		,37 AS cnt37
                |		,38 AS cnt38
                |		,39 AS cnt39
                |		,40 AS cnt40
                |		,41 AS cnt41
                |		,42 AS cnt42
                |		,43 AS cnt43
                |		,44 AS cnt44
                |		,45 AS cnt45
                |		,46 AS cnt46
                |		,47 AS cnt47
                |		,48 AS cnt48
                |		,49 AS cnt49
                |		,50 AS cnt50
                |		,51 AS cnt51
                |		,52 AS cnt52
                |		,53 AS cnt53
                |		,54 AS cnt54
                |		,55 AS cnt55
                |		,56 AS cnt56
                |		,57 AS cnt57
                |		,58 AS cnt58
                |		,59 AS cnt59
                |		,60 AS cnt60
                |		,61 AS cnt61
                |		,62 AS cnt62
                |		,63 AS cnt63
                |		,64 AS cnt64
                |		,65 AS cnt65
                |		,66 AS cnt66
                |		,67 AS cnt67
                |		,68 AS cnt68
                |		,69 AS cnt69
                |		,70 AS cnt70
                |		,71 AS cnt71
                |		,72 AS cnt72
                |		,73 AS cnt73
                |		,74 AS cnt74
                |		,75 AS cnt75
                |		,76 AS cnt76
                |		,77 AS cnt77
                |		,78 AS cnt78
                |		,79 AS cnt79
                |		,80 AS cnt80
                |		,81 AS cnt81
                |		,82 AS cnt82
                |		,83 AS cnt83
                |		,84 AS cnt84
                |		,85 AS cnt85
                |		,86 AS cnt86
                |		,87 AS cnt87
                |		,88 AS cnt88
                |		,89 AS cnt89
                |		,90 AS cnt90
                |		,91 AS cnt91
                |		,92 AS cnt92
                |		,93 AS cnt93
                |		,94 AS cnt94
                |		,95 AS cnt95
                |		,96 AS cnt96
                |		,97 AS cnt97
                |		,98 AS cnt98
                |		,99 AS cnt99
                |		,100 AS cnt100
                |	FROM lineitem
                |	WHERE l_shipdate <= '1998-09-01'
                |	) AS A
                |GROUP BY l_returnflag
                |	,l_linestatus
              """.stripMargin
  )

}
