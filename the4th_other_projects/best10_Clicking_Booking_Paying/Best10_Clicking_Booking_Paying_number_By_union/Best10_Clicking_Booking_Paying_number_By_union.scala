package sparkProject.the4th_other_projects.best10_Clicking_Booking_Paying.Best10_Clicking_Booking_Paying_number_By_union

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Best10_Clicking_Booking_Paying_number_By_union {
  /*
    1. connect to Spark
    2. filter and map the data to (category_id, clicking_number) and (category_id, booking_number) and (category_id, paying_number)
    3. map the data:
        if the action is clicking, then map the data to tuple of RDD(category_id, (clicking_number,0,0))
        if the action is booking,  then map the data to tuple of RDD(category_id, (0,booking_number,0))
        if the action is paying,   then map the data to tuple of RDD(category_id, (0,0,paying_number))
    4. by using the union function to union above three tuple to a new tuple of RDD
    5. by reducing these new tuple of RDD to
        (category_id, (clicking_number, booking_number, paying_number)) by key by adding the elements together.
    6. sort the tuple by (clicking_number, booking_number, paying_number)
    7. print on the console


     */
  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparConf)

    val dataRDD: RDD[String] = sc.textFile("data/user_visit_action.txt")

    dataRDD.cache()
    // count the clicking number
    val clickedRDD: RDD[String] = dataRDD.filter(
      line => {
        val lineArray: Array[String] = line.split("_")
        lineArray(6) != "-1"
      }
    )

    val clickedCountedRDD: RDD[(String, Int)] = clickedRDD.map(
      line => {
        val lineArray: Array[String] = line.split("_")
        (lineArray(6), 1)
      }
    ).reduceByKey(_ + _)

    // count the booking number
    val bookedRDD: RDD[String] = dataRDD.filter(
      line => {
        val lineArray: Array[String] = line.split("_")
        lineArray(8) != "null"
      }
    )

    val bookingCountedRDD: RDD[(String, Int)] = bookedRDD.flatMap(
      line => {
        val lineArray: Array[String] = line.split("_")
        val bookingSplitArray: Array[String] = lineArray(8).split(",")
        bookingSplitArray.map(
          s => {
            (s, 1)
          }
        )
      }
    ).reduceByKey(_ + _)

    // count the pay number
    val pay_filteredRDD: RDD[String] = dataRDD.filter(
      line => {
        val lineArray: Array[String] = line.split("_")
        lineArray(10) != "null"
      }
    )

    val payingCountedRDD: RDD[(String, Int)] = pay_filteredRDD.flatMap(
      line => {
        val lineArray: Array[String] = line.split("_")
        val StringSplitArray: Array[String] = lineArray(10).split(",")
        StringSplitArray.map(
          s => {
            (s, 1)
          }
        )
      }
    ).reduceByKey(_ + _)

    //map the counted number (category_id, clicking_number) to these type: (category_id, (clicking_number, 0,0))
    // clickedCountedRDD: RDD[(String, Int)]
    val clicking_New_type: RDD[(String, (Int, Int, Int))] = clickedCountedRDD.map {
      case (str, click_cnt) => {
        (str, (click_cnt, 0, 0))
      }
    }

    val booking_new_type: RDD[(String, (Int, Int, Int))] = bookingCountedRDD.map {
      case (str, book_cnt) => {
        (str, (0, book_cnt, 0))
      }
    }

    val paying_new_type: RDD[(String, (Int, Int, Int))] = payingCountedRDD.map {
      case (str, pay_cnt) => {
        (str, (0, 0, pay_cnt))
      }
    }

    // union the new type and reduce to the type of tuple: (category_id, (clicking_number, booking_number,paying_number())
    val click_book_pay_counted: RDD[(String, (Int, Int, Int))] = clicking_New_type.union(booking_new_type).union(paying_new_type)

    val resultRDD: RDD[(String, (Int, Int, Int))] = click_book_pay_counted.reduceByKey(
      (tup1, tup2) => {
        (tup1._1 + tup2._1, tup1._2 + tup2._2, tup1._3 + tup2._3)
      }
    )
    //resultRDD.take(50).foreach(x => println(x))
    resultRDD.sortBy(_._2, ascending = false).take(50).foreach(x => println(s"category_id: ${x._1} clicking number:${x._2._1} booking number:${x._2._2} paying number:${x._2._3}"))
    sc.stop()
  }

}
