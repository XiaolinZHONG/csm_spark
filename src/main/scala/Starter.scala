import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.sql.SparkSession

object Starter{
  def main(args: Array[String]): Unit = {
    val spark = new SparkSession.Builder().master("local").getOrCreate()
    val rfc= new RandomForestClassifier()
    val path="../../Desktop/dataAnalysis/ctrip/test.csv"
    val df = spark.read.format("csv").option("header","true").load(path)
//    df.show(5)


    val df2= Utils.readCsv(path,spark)
    df2.createOrReplaceTempView("test")
    spark.sql(" SELECT * FROM test")
//    spark.sql("CREATE TABLE test (id int , name string) " +
//      "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE ")
//    spark.sql("LOAD DATA LOCAL INPATH '../../Desktop/dataAnalysis/ctrip/test.txt' INTO TABLE test")
//    val sqldf=spark.sql("SELECT uid_grade, uid_age," +
//  "case when uid_age=-1 then 'F' " +
//  "else (case when uid_age>35 then 'O' else 'Y' end) " +
//  "end as ageCheck FROM test")
    // 选择一列进行多项比较给出新的一列
//    sqldf.show()

  }
}