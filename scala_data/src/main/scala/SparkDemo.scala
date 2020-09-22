
import org.apache.spark.sql.{Row,SparkSession,DataFrameWriter,SaveMode}
import DqCheckwithDeequ.{dqCheck}

object SparkDemo {
  /* 这是我的第一个 Scala 程序
   */
  def main(args: Array[String]) {
    val spark=SparkSession
      .builder()
      .appName("SparkSQLDemo")
      .master("local")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    val df=spark.read.option("header",true).csv("/Users/Cindy_Sun/testdata/source/margin_000001.csv")
    df.createOrReplaceTempView("CSV_TEMP")
    spark.sql("select * from CSV_TEMP").show()
    val df1:org.apache.spark.sql.DataFrame=df.withColumn("2019",df("2019").cast("Integer"))
    df1.printSchema()
    dqCheck(spark,df1,"2019")
    df1.write.mode(SaveMode.Overwrite).parquet("/Users/Cindy_Sun/testdata/source/margin/")
    spark.stop()
  }
}