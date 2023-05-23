package creditcard_sparkProject.frameWork_Best10.util

import org.apache.spark.SparkContext

object BufferUtil {
  val threadLocal = new ThreadLocal[SparkContext]()

  def get() ={
    threadLocal.get()
  }

  def put(sc: SparkContext)= {
    threadLocal.set(sc)
  }

  def clear() ={
    threadLocal.remove()
  }
}
