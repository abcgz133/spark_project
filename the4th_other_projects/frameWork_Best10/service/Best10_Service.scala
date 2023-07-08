package creditcard_sparkProject.the4th_other_projects.frameWork_Best10.service

import exercise.frameWork_Best10.comm.Trait_Best10_Service
import exercise.frameWork_Best10.dao.Best10_DAO
import org.apache.spark.rdd.RDD

class Best10_Service extends Trait_Best10_Service{
  private val best10_Dao = new Best10_DAO()
  def best10_analysis() ={
    val dataRDD: RDD[String] = best10_Dao.ReadFile("data/user_visit_action.txt")
    val new_type_tuple: RDD[(String, (Int, Int, Int))] = dataRDD.flatMap(
      line => {
        val lineArray: Array[String] = line.split("_")
        if (lineArray(6) != "-1") {
          List((lineArray(6), (1, 0, 0)))
        } else {
          if (lineArray(8) != "null") {
            val bookingArray: Array[String] = lineArray(8).split(",")
            bookingArray.map(
              str => {
                (str, (0, 1, 0))
              }
            )
          } else {
            if (lineArray(10) != "null") {
              val payingArray: Array[String] = lineArray(10).split(",")
              payingArray.map(
                str => {
                  (str, (0, 0, 1))
                }
              )
            } else {
              Nil
            }
          }
        }
      }
    )

    val resultRDD: RDD[(String, (Int, Int, Int))] = new_type_tuple.reduceByKey(
      (tup1, tup2) => {
        (tup1._1 + tup2._1, tup1._2 + tup2._2, tup1._3 + tup2._3)
      }
    )

    //resultRDD.take(50).foreach(x => println(x))
    val tuplesArray: Array[(String, (Int, Int, Int))] = resultRDD.sortBy(_._2, ascending = false).take(50)
    tuplesArray

    //.foreach(x => println(s"category_id: ${x._1} clicking number:${x._2._1} booking number:${x._2._2} paying number:${x._2._3}"))

  }
}
