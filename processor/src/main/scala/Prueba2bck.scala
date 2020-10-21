import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, datediff, substring, to_date}
import org.apache.spark.sql.types._


object Prueba2bck  {

  def main (arg: Array[String]): Unit = {
    println("ini - Prueba2")
    val conf = new SparkConf().setAppName("rascacielos").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ss = SparkSession.builder
              .master("local[2]")
              .appName("rascacielos")
              //.config("spark.some.config.option", "some-value")
              .getOrCreate()

    val csvRDD = sc.textFile("file:///home/alberto/workspace2/rascacielos/data/test_BCN_MAD_JSON.csv")
    print(csvRDD.count())

    /*

    //find the rows which have only one digit in the 7th column in the CSV
    // val rdd1 = rdd.filter(s => s.split(",")(6).length() == 1)

    // rdd1.saveAsTextFile("wasb:///HVACout")
     */

    /*
    // The schema is encoded in a string
    val schemaString = "flight_date_str extraction_date_time price flight_number airline departure_time arrival_time origin_airport destination_airport"

    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val simpleSchema = StructType(fields)
     */

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


    // Apply the schema to the RDD
    val flightsDF = ss.createDataFrame(filteredRDD, schema)
                        .withColumn("flight_date", to_date(col("flight_date_str"),"yyyy-MM-dd"))
                        .withColumn("extraction_date", to_date(substring(col("extraction_date_time"), 0, 10), "yyyy-MM-dd"))
                        .withColumn("lag", datediff(col("flight_date"), col("extraction_date")))
                        .drop("flight_date_str")
    println(flightsDF.head())

    // Creates a temporary view using the DataFrame
    flightsDF.createOrReplaceTempView("flights")

    flightsDF.printSchema()



    // SQL can be run over a temporary view created using DataFrames
    val results = ss.sql("SELECT count(3) FROM flights where flight_date < to_date(\"2020-10-18\",\"yyyy-MM-dd\")")
    results.collect().take(5).foreach(println)

    println(results.head())

    println("fin - Prueba2")
  }

}
