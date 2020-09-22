import com.amazon.deequ.analyzers.runners.{AnalysisRunner,AnalyzerContext}
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import com.amazon.deequ.analyzers.{Compliance,Correlation,Size,Completeness,Mean,Maximum}
import org.apache.spark.sql.{DataFrameWriter, Row,Column,SaveMode, SparkSession}
import org.apache.spark.sql.SparkSession


case object DqCheckwithDeequ{
  def dqCheck(spark:org.apache.spark.sql.SparkSession,dataframe: org.apache.spark.sql.DataFrame,col_str:String ) {
    val analysisResult: AnalyzerContext ={AnalysisRunner.onData(dataframe)
      .addAnalyzer(Size())
      .addAnalyzer(Mean(col_str))
      .addAnalyzer(Maximum(col_str))
      .addAnalyzer(Completeness(col_str))
      .run()
        }
    val metrics = successMetricsAsDataFrame(spark, analysisResult)
    val metrics1:org.apache.spark.sql.DataFrame=metrics.withColumn("value",metrics("value").cast(org.apache.spark.sql.types.DecimalType(10,3)))
    metrics1.show()
  }
}
