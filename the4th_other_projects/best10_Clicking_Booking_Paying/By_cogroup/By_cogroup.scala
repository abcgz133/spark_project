package creditcard_sparkProject.the4th_other_projects.best10_Clicking_Booking_Paying.By_cogroup

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object By_cogroup {
  /*
    1. connect to Spark
    2. filter and map the data to (category_id, clicking_number) and (category_id, booking_number) and (category_id, paying_number)
    3. by using the cogroup function to form the data to this tuple:
        (category_id, (clicking_number, booking_number, paying_number))
    4. sort the tuple by (clicking_number, booking_number, paying_number)
    5. print on the console


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
    val payingfilteredRDD: RDD[String] = dataRDD.filter(
      line => {
        val lineArray: Array[String] = line.split("_")
        lineArray(10) != "null"
      }
    )

    val payingCountRDD: RDD[(String, Int)] = payingfilteredRDD.flatMap(
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

    //co-group these 3 tuples to form to this type of tuple:
    //        (category_id, (clicking_number, booking_number, paying_number))
    val groupedRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickedCountedRDD.cogroup(bookingCountedRDD, payingCountRDD)

    // to map the Iterable type to  Int
    val resultRDD: RDD[(String, (Int, Int, Int))] = groupedRDD.mapValues {
      case (clickIter, bookIter, payIter) => {
        var clickIterCnt = 0
        var bookIterCnt = 0
        var payIterCnt = 0
        val clickIterator: Iterator[Int] = clickIter.iterator
        if (clickIterator.hasNext) {
          clickIterCnt = clickIterator.next()
        }

        val bookIterator: Iterator[Int] = bookIter.iterator
        if (bookIterator.hasNext) {
          bookIterCnt = bookIterator.next()
        }

        val payIterator: Iterator[Int] = payIter.iterator
        if (payIterator.hasNext) {
          payIterCnt = payIterator.next()
        }

        (clickIterCnt, bookIterCnt, payIterCnt)
      }
    }

    //resultRDD.take(50).foreach(x => println(x))
    resultRDD.sortBy(_._2, ascending = false).take(50).foreach(x => println(s"category_id: ${x._1} clicking number:${x._2._1} booking number:${x._2._2} paying number:${x._2._3}"))
    sc.stop()
  }

}
