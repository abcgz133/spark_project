package creditcard_sparkProject.the4th_other_projects.best10_Clicking_Booking_Paying.By_Accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object By_Accumulator {
  /*
  1. define the connection to Spark
  2. define the accumulator extends AccumulatorV2
  3. parse the data
  4. acquire the value from the accumulator
  5. sort the value
  6. print in the console

 */

  //1. define the connection to Spark

  def main(args: Array[String]): Unit = {
    //  1. define the connection to Spark
    val sparConf = new SparkConf().setMaster("local[*]").setAppName("Top10Analysis")
    val sc = new SparkContext(sparConf)
    val sumAccumulator = new SumAccumulator()
    sc.register(sumAccumulator, "ItemAccumulator")

    // 3. parse the data
    val dataRDD: RDD[String] = sc.textFile("data/user_visit_action.txt")

    dataRDD.foreach(
      line => {
        val lineArray: Array[String] = line.split("_")
        if (lineArray(6) != "-1") {

          sumAccumulator.add(lineArray(6), "click")
        } else if (lineArray(8) != "null") {
          val bookArray: Array[String] = lineArray(8).split(",")
          bookArray.foreach(
            bookcid => {
              sumAccumulator.add(bookcid, "book")
            }
          )
        } else if (lineArray(10) != "null") {
          val payArray: Array[String] = lineArray(10).split(",")
          payArray.foreach(
            paycid => {
              sumAccumulator.add(paycid, "pay")
            }
          )
        }
      }
    )


    // 4. acquire the value from the accumulator
    val sumFinalMap: mutable.Map[String, Item] = sumAccumulator.value
    val sumFinalIterable: mutable.Iterable[Item] = sumFinalMap.map(_._2)

    // 5. sort the value
    val sortedList: List[Item] = sumFinalIterable.toList.sortWith(
      (left, right) => {
        if (left.click_cnt > right.click_cnt) {
          true
        } else if (left.click_cnt == right.click_cnt) {
          if (left.book_cnt > right.book_cnt) {
            true
          } else {
            left.pay_cnt > right.pay_cnt
          }
        } else false
      }
    )

    println("the best 10 clicking_booking_paying:")
    //  6. print in the console
    sortedList.take(10).foreach(x => println(s"category_id:${x.cid}, (clicking_cnt:${x.click_cnt} booking:${x.book_cnt} paying_cnt:${x.pay_cnt} )"))
    sc.stop()
  }


  // 2. define the accumulator and a cass class
  case class Item(var cid: String, var click_cnt: Int, var book_cnt: Int, var pay_cnt: Int)

  /*
  In: (cid, action)  Out: Map[cid, Item[cid, click_cnt, book_cnt, pay_cnt]]
   */
  class SumAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, Item]] {
    private val sumMap: mutable.Map[String, Item] = mutable.Map[String, Item]()

    override def isZero: Boolean = {
      sumMap.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, Item]] = {
      new SumAccumulator()
    }

    override def reset(): Unit = {
      sumMap.clear()
    }

    override def add(v: (String, String)): Unit = {
      val cidStr: String = v._1
      val actionStr: String = v._2
      val addItem: Item = sumMap.getOrElse(cidStr, Item(cidStr, 0, 0, 0))
      if (actionStr == "click") {
        addItem.click_cnt += 1
      } else if (actionStr == "book") {
        addItem.book_cnt += 1
      } else if (actionStr == "pay") {
        addItem.pay_cnt += 1
      }
      sumMap.update(cidStr, addItem)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, Item]]): Unit = {
      val thisMap: mutable.Map[String, Item] = this.value
      val otherMap: mutable.Map[String, Item] = other.value

      otherMap.foreach {
        case (str, iter) => {
          val mergeItem: Item = thisMap.getOrElse(str, Item(str, 0, 0, 0))
          mergeItem.click_cnt += iter.click_cnt
          mergeItem.book_cnt += iter.book_cnt
          mergeItem.pay_cnt += iter.pay_cnt
          thisMap.update(str, mergeItem)
        }
      }
    }

    override def value: mutable.Map[String, Item] = {
      sumMap
    }
  }
}
