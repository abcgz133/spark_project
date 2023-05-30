package creditcard_sparkProject.best10_Clicking_Booking_Paying.Best10_Clicking_Booking_Paying_number_By_New_Tuple_type

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Best10_Clicking_Booking_Paying_number_By_New_Tuple_type {
  /*
    1. connect to Spark
    2. filter and map the data to New_tuple_type
        if clicking: (category_id, 1, 0, 0)
        if booking: (category_id, 0, 1, 0)
        if paying: (category_id, 0, 0, 1)
    3. reduce the new_tuple_type
    4. sort the tuple by (clicking_number, booking_number, paying_number)
    5. print on the console
     */
  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparConf)

    val dataRDD: RDD[String] = sc.textFile("data/user_visit_action.txt")

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
    resultRDD.sortBy(_._2, ascending = false).take(50).foreach(x => println(s"category_id: ${x._1} clicking number:${x._2._1} booking number:${x._2._2} paying number:${x._2._3}"))
    sc.stop()
  }

}
