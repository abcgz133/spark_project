package creditcard_sparkProject.frameWork_Best10.comm

import exercise.frameWork_Best10.util.BufferUtil
import org.apache.spark.{SparkConf, SparkContext}

trait Trait_Best10_Application {
  def start(master:String = "local[*]", appName: String = "HotCategoryTop10Analysis")(best10_app: => Unit) {
    val sparConf = new SparkConf().setMaster(master).setAppName(appName)
    val sc = new SparkContext(sparConf)
    BufferUtil.put(sc)
    try {
      best10_app
    } catch {
      case ex => println(ex.getMessage)
    }

    sc.stop()
    BufferUtil.clear()
  }
}
