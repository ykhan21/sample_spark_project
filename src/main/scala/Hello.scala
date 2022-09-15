import org.apache.spark.sql.SparkSession

object Hello extends App {
  println("Hel lo!")


  val spark = SparkSession.builder
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  val data = Seq(
    ("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  )

  val columns = Seq("firstname","lastname","country","state")

  import spark.implicits._
  val df = data.toDF(columns:_*)

  df.show(false)

  df.printSchema

  val newDF = df.drop("firstname")

  newDF.show(false)

  newDF.printSchema
}