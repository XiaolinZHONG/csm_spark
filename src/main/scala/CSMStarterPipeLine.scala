


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by zhongxl on 2016/10/13.
  */
object CSMStarterPipeLine {

  var sc: SparkContext = null
  var sqlContext: SparkSession = null
  var hiveContext: SparkSession = null
  val baseOutputPath = "/user/bifinread/csm/"

  /**
    * 训练模型
    * 保存模型与Scaler对象
    *
    * @param moduleName
    * @return
    */
  def train(moduleName: String, dataPath: String) = {
    val startTime = System.currentTimeMillis()
    val data = Utils.readCsv(dataPath, sqlContext)
    moduleName match {
      case "consume" => new ConsumeModel().dataTraining(data)
//      case "finance" => new FinanceModel().dataTraining(data)
//      case "interaction" => new InteractionModel().dataTraining(data)
      case "people" => new PeopleModel().dataTraining(data)
//      case "relation" => new RelationModel().dataTraining(data)
      case "all" => {
        new ConsumeModel().dataTraining(data)
//        new FinanceModel().dataTraining(data)
//        new InteractionModel().dataTraining(data)
        new PeopleModel().dataTraining(data)
//        new RelationModel().dataTraining(data)
      }
    }
    val endTime = System.currentTimeMillis()
    println("TRAIN TIME COST： " + (endTime - startTime) + "ms")
  }

  /**
    * 预测结果
    *
    * @param moduleName
    * @param dataPath
    * @return
    */
  def predict(moduleName: String, dataPath: String): Unit = {
    val data = hiveContext.sql(s"SELECT * FROM ${dataPath} WHERE uid_uid IS NOT NULL")

    def presistModuleDF = (module: String, df: DataFrame) => {
      df.registerTempTable(module)
      hiveContext.sql(s"DROP TABLE if EXISTS tmp_paydb.tmp_sp_csm_${module}")
      hiveContext.sql(s"CREATE TABLE tmp_paydb.tmp_sp_csm_${module} AS SELECT * FROM ${module}")
    }

    moduleName match {
      case "consume" => presistModuleDF("consume", new ConsumeModel().dataPredict(data))
//      case "finance" => presistModuleDF("finance", new FinanceModel().dataPredict(data))
//      case "interaction" => presistModuleDF("interaction", new InteractionModel().dataPredict(data))
      case "people" => presistModuleDF("people", new PeopleModel().dataPredict(data))
//      case "relation" => presistModuleDF("relation", new RelationModel().dataPredict(data))
      case "all" => {
        predict("consume", dataPath)
        predict("finance", dataPath)
        predict("interaction", dataPath)
        predict("people", dataPath)
        predict("relation", dataPath)
      }
    }
  }


  def main(args: Array[String]): Unit = {

    val appName = args(0)
    val appType = args(1) // train,predict,combine
    val moduleName = args(2) // sub-model name
    val dataPath = args(3)

    val conf = new SparkConf().setAppName(appName).set("spark.speculation", "false").setMaster("local")
    Logger.getRootLogger.setLevel(Level.WARN)
    //.setMaster("local")

    val spark=SparkSession.builder().master("local")
      .config("spark.sql.warehouse.dir","file:///")
      .getOrCreate()


//    sc = new SparkContext(conf)
    sqlContext = spark
    hiveContext = spark

    appType match {
      case "train" => train(moduleName, dataPath)
      case "predict" => predict(moduleName, dataPath)
    }
  }

  //---------------------------------------------------------------//
}
