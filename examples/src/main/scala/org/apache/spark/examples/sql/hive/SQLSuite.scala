package org.apache.spark.examples.sql.hive

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SQLSuite {
  val sparkConf = new SparkConf().setAppName("SQL Suite")
  val sparkContext = new SparkContext(sparkConf)

  val master = sparkConf.getenv("SPARK_MASTER_IP")
  val hdfs = s"hdfs://$master:9010"

  val hiveContext = new HiveContext(sparkContext)

  val sharedConf = Seq(
    SQLConf.SHUFFLE_PARTITIONS -> s"${8 * sparkConf.get("spark.number.executors").toInt}",
    SQLConf.CODEGEN_ENABLED -> "false",
    "online.sql.number.bootstrap.trials" -> "100"
  )
  sharedConf.foreach { case (key, value) =>
    hiveContext.setConf(key, value)
  }

  val scale: Int = 10 / 10

  val specConfs = {
    val default = Seq("online.sql.sampled.relations" -> "lineitem", "online.sql.number.batches" -> s"${10 * scale}")
    val confs = Seq(
      Seq("online.sql.sampled.relations" -> "partsupp", "online.sql.number.batches" -> s"${3 * scale}"),
      Seq("online.sql.sampled.relations" -> "customer", "online.sql.number.batches" -> s"${3 * scale}")
    )
    Map("Q11" -> confs(0), "Q16" -> confs(0), "Q22" -> confs(1))
      .withDefault(_ => default)
  }

  var debug = false

  def main(args: Array[String]): Unit = {
    val benchmark = "benchmark(.*)".r
    args(0) match {
      case "create" => create(args(1))
      case "drop" => drop()
      case "show" => show()
      case "test" => test()
      case benchmark(option) =>
        val queryName = s"Q${args(1).toInt}"

        // Warm up
        option match {
          case "-cache" =>
            tablesUsed(queryName).foreach { table =>
              hiveContext.cacheTable(table)
              cacheRun(s"Q$table")
            }
          case _ =>
        }
        cacheRun(queryName)

        Range(0, 4).foreach { i =>
          println(s"REMARK: Running $queryName baseline #$i...")
          hiveRun(queryName)
        }
    }
  }

  def cacheRun(queryName: String): Unit = {
    val query = hiveContext.sql(queries(queryName))
    query.collect()
  }

  def hiveRun(queryName: String): Unit = {
    val query = hiveContext.sql(queries(queryName))
    printPlan(query)

    printResult(query)
  }

  def printResult(query: DataFrame): Unit = {
    val rdd = query.rdd // force to initialize the query plan

    var rows: Array[Row] = null
    benchmark {
      rows = rdd.collect()
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

  val cacheTemplate = """
                        |SELECT count(1)
                        |FROM TABLE
                      """.stripMargin

  val tablesUsed = Map(
    "Q1" -> Seq("lineitem"),
    "Q3" -> Seq("lineitem", "customer", "orders"),
    "Q5" -> Seq("lineitem", "customer", "orders", "supplier", "nation", "region"),
    "Q6" -> Seq("lineitem"),
    "Q7" -> Seq("lineitem", "customer", "orders", "supplier", "nation"),
    "Q8" -> Seq("lineitem", "customer", "orders", "supplier", "part", "nation", "region"),
    "Q9" -> Seq("lineitem", "part", "orders", "supplier", "nation", "partsupp"),
    "Q10" -> Seq("lineitem", "customer", "orders", "nation"),
    "Q11" -> Seq("partsupp", "supplier", "nation"),
    "Q12" -> Seq("lineitem", "orders"),
    "Q14" -> Seq("lineitem", "part"),
    "Q16" -> Seq("partsupp", "supplier", "part"),
    "Q17" -> Seq("lineitem", "part"),
    "Q18" -> Seq("lineitem", "customer", "orders"),
    "Q19" -> Seq("lineitem", "part"),
    "Q20" -> Seq("lineitem", "supplier", "nation", "partsupp", "part"),
    "Q22" -> Seq("customer")
  )

  val queries = Map(
    "Qlineitem" -> cacheTemplate.replace("TABLE",  "lineitem"),
    "Qorders" -> cacheTemplate.replace("TABLE",  "orders"),
    "Qcustomer" -> cacheTemplate.replace("TABLE",  "customer"),
    "Qsupplier" -> cacheTemplate.replace("TABLE",  "supplier"),
    "Qpartsupp" -> cacheTemplate.replace("TABLE",  "partsupp"),
    "Qpart" -> cacheTemplate.replace("TABLE",  "part"),
    "Qnation" -> cacheTemplate.replace("TABLE",  "nation"),
    "Qregion" -> cacheTemplate.replace("TABLE",  "region"),
    "Q0" -> """
              |SELECT l_returnflag
              | ,l_linestatus
              | ,sum(l_quantity)
              |FROM lineitem
              |WHERE l_shipdate <= '1998-09-01'
              |GROUP BY l_returnflag
              | ,l_linestatus
            """.stripMargin,
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
    "Q11" -> s"""
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
               |			,sum(ps_supplycost * ps_availqty) * ${0.0002 / scale} AS threshold
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
    "Q18" -> s"""
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
               |WHERE tot_qty > ${305025000 * scale}
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
    "Q20" -> s"""
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
               |				,sum(l_quantity) * ${0.001 / scale} AS qty
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
             """.stripMargin
  )

}
