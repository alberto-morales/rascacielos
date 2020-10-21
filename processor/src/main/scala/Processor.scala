import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, datediff, substring, to_date, lit, expr}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Processor  {

  def main (arg: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("rascacielos").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ss = SparkSession.builder
      .master("local[2]")
      .appName("rascacielos")
      .getOrCreate()

    val csvRDD = sc.textFile("file:///home/alberto/workspace2/rascacielos/data/test_LCG_MAD_JSON.csv")
    print(csvRDD.count())

    val schema = StructType(Array(
      StructField("flight_date_str",StringType,true),
      StructField("extraction_date_time",StringType,true),
      StructField("price",DoubleType,true),
      StructField("flight_number", StringType, true),
      StructField("airline", StringType, true),
      StructField("departure_time", StringType, true),
      StructField("arrival_time", StringType, true),
      StructField("origin_airport", StringType, true),
      StructField("destination_airport", StringType, true)
    ))

    // Convert records of the RDD (flights) to Rows
    val flightsRDD = csvRDD
      .map(_.split("<#>"))
      .map(attributes => Row(attributes(0), attributes(1).trim, attributes(2).toDouble, attributes(3),
        attributes(4), attributes(5), attributes(6), attributes(7),
        attributes(8)))

    val filteredRDD = flightsRDD.filter(_.getString(4) != "Renfe")
      .filter(_.getString(6).indexOf("+") < 0)


    val toInt = udf[Int, String]( _.toInt)


    val primerDF = ss.createDataFrame(filteredRDD, schema);
    primerDF.printSchema();
    println(primerDF.head())

    // Apply the schema to the RDD
    val flightsDF = ss.createDataFrame(filteredRDD, schema)
      .withColumn("flight_date", to_date(col("flight_date_str"),"yyyy-MM-dd"))
      .withColumn("extraction_date", to_date(substring(col("extraction_date_time"), 0, 10), "yyyy-MM-dd"))
      .withColumn("lag", datediff(col("flight_date"), col("extraction_date")))
      .withColumn("departure_hour", toInt(substring(col("departure_time"), 0, 2)))
      .withColumn("arrival_hour", toInt(substring(col("arrival_time"), 0, 2)))
      .withColumn("duration", expr("arrival_hour - departure_hour"))
      .drop("flight_date_str")
      .drop("departure_hour")
      .drop("arrival_hour")

    flightsDF.printSchema()
    println(flightsDF.head())

    // Creates a temporary view using the DataFrame
    // flightsDF.createOrReplaceTempView("flights")

    val departedDF = flightsDF
      .filter(flightsDF("flight_date").lt(lit("2020-10-18")))
      .filter(flightsDF("duration").lt(4))
    // departedDF.createOrReplaceTempView("departed")

    val minPrices = departedDF.groupBy("flight_date").min("price")
      .withColumnRenamed("flight_date", "fd")
      .withColumnRenamed("min(price)", "minPrice")
    minPrices.createOrReplaceTempView("minPrices")

    import ss.implicits._
    val enlazados = departedDF.join(minPrices).where($"flight_date" === $"fd")
      .drop("fd")
    enlazados.printSchema()
    println(enlazados.head())

    // enlazados.write.format("csv").save("file:///home/alberto/workspace2/rascacielos/data/test_BCN_MAD_processed.csv")

    enlazados.coalesce(1)
      .write
      .option("header","true")
      .option("sep",",")
      .mode("overwrite")
      .csv("file:///home/alberto/workspace2/rascacielos/data/test_LCG_MAD_processed_x.csv")

  }

}
