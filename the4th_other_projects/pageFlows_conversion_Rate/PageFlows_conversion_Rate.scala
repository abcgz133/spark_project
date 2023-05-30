package sparkProject.the4th_other_projects.pageFlows_conversion_Rate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PageFlows_conversion_Rate {
  /*
 1. connect to Spark
 2. parse the data to case class
 3. to compute the rate of nominator:
     A. group by session_id,
     B. sort the transaction_date
     C. form the List of pages
     D. form the consecutive pages to a List by zipping with id, and the tail of id
     E. map and reduce

 4. to compute the rate of denominator:
     A. form the List of pages
     B. map and reduce
  */

  def main(args: Array[String]): Unit = {
    // connect to Spark
    val sparConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparConf)

    //parse the data to case class
    val actionRDD = sc.textFile("data/user_visit_action.txt")
    val dataUserActionRDD: RDD[UserVisitAction] = actionRDD.map(
      line => {
        val actionArray: Array[String] = line.split("_")
        UserVisitAction(
          actionArray(0),
          actionArray(1).toLong,
          actionArray(2),
          actionArray(3).toLong,
          actionArray(4),
          actionArray(5),
          actionArray(6).toLong,
          actionArray(7).toLong,
          actionArray(8),
          actionArray(9),
          actionArray(10),
          actionArray(11),
          actionArray(12).toLong
        )
      }
    )

    dataUserActionRDD.cache()

    // to compute the rate of nominator
    //  group by session_id
    val groupedRDD: RDD[(String, Iterable[UserVisitAction])] = dataUserActionRDD.groupBy(_.session_id)
    //  form the consecutive pages to a List by zipping with id, and the tail of id
    val consecutivePageTupleRDD: RDD[(String, List[((Long, Long), Int)])] = groupedRDD.mapValues(
      itr => {
        val pagesList: List[Long] = itr.toList.sortBy(_.action_time).map(_.page_id)
        pagesList.zip(pagesList.tail).map(
          t => (t, 1)
        )
      }
    )




    val consecutivePagesArray: Array[((Long, Long), Int)] = consecutivePageTupleRDD.map(_._2).flatMap(list => list).reduceByKey(_ + _).collect()

    //to compute the rate of denominator:
    val eachPageMap: Map[Long, Long] = dataUserActionRDD.map(_.page_id).map(
      t => (t, 1L)
    ).reduceByKey(_ + _).collect().toMap

    println("the rate of page_flow is ")
    var rank = 0
    consecutivePagesArray.map {
      case ((page1, page2), cnt) => {
        val cntDenominator2: Long = eachPageMap.getOrElse(page1, 0L)
        ((page1, page2), cnt.toDouble / cntDenominator2.toDouble)
      }
    }.toList.sortBy(_._2)(Ordering.Double.reverse).take(50).foreach(x => {
      rank += 1
      println(s"the rate of from page ${x._1._1} to page ${x._1._2}: ${x._2.formatted("%.5f")} and the rank: ${rank} ")
    })



    sc.stop()

  }

  case class UserVisitAction(
                              date: String, //the date of user click
                              user_id: Long, //user id
                              session_id: String, //Session ID
                              page_id: Long, //PageID
                              action_time: String, //action time
                              search_keyword: String, //key word in search
                              click_category_id: Long, // category ID of selling goods
                              click_product_id: Long, // product ID
                              order_category_ids: String, //category ids in one ordering
                              order_product_ids: String, // product_ids in one ordering
                              pay_category_ids: String, // category ids in one paying
                              pay_product_ids: String, // product_ids in one paying
                              city_id: Long    // city id
                            )
}
