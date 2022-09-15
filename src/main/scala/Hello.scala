import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, upper}


object Hello extends Logging {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = initiateSpark()
    val df = loadData(spark, "src/main/scala/sample.csv", getOptions)
    df.show()
    df.printSchema()
    val df2 = df.withColumn("Trip Seconds", col("Trip Seconds").cast("int"))
    df2.show()
    df2.printSchema()
    val list = Seq(col("Company"),col("Trip Seconds"))
    val df3 = df.select(col("Company"),col("Trip Seconds").cast("int"))
    df3.show()
    df3.printSchema()
    val df4 = df3.select(upper(list(0)), list(1))
    df4.show()
    df4.printSchema()

  }
  def getOptions = Map(
    "format" -> "csv",
    "header" -> "true",
    "mode" -> "DROPMALFORMED",
    "parserLib" -> "univocity"
  )

  private def initiateSpark(): SparkSession = {
    val server_address = "127.0.0.1"
    val app_name = "spark-text"
    val app_mode = "local[4]"

    val spark = SparkSession
      .builder()
      .master(app_mode)
      .appName(app_name)
      .config("spark.driver.bindAddress", server_address)
      .getOrCreate()

    // initiate spark context
    val sc = spark.sparkContext

    spark
  }
  def loadData(sc: SparkSession, dataPath: String, options: Map[String, String]): DataFrame = {
    try {
      sc.read
        .format("csv")
        .options(options)
        .load(dataPath)
    } catch {
      case e: Exception => log.error(s"Error loading file ${}")
        null
    }
  }

}