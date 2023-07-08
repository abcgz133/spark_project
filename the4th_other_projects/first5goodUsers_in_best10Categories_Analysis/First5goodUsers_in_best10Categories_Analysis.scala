package creditcard_sparkProject.the4th_other_projects.first5goodUsers_in_best10Categories_Analysis

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object First5goodUsers_in_best10Categories_Analysis {
  /*
    1. define the method(best10Category) to get the best 10 category
    2. parse the textFile to data
    3. filter the data by data whether are contained in best10Categories and is "click" transaction
    4. to map the data to a Tuple[(category_id, session_id),1L] and reduce the Tuple to ((category_id,sessionId), sum)
    5. to reform the result  from ((category_id,sessionId), sum) to (category_id,（sessionId, sum）)
    6. group by the key for (category_id,(sessionId, sum)), and sort the List(sessionId, sum) by sum
    7. print the result by take first 5 of List(sessionId, sum)
     */

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparConf)
    val dataRDD: RDD[String] = sc.textFile("data/user_visit_action.txt")
    dataRDD.cache()

    val best10Array: Array[String] = best10Categories(dataRDD)

    //filter the data by whether in best10Categories
    val filteredRDD: RDD[String] = dataRDD.filter(
      line => {
        val lineArray: Array[String] = line.split("_")
        if (lineArray(6) != "-1") {
          best10Array.contains(lineArray(6))
        } else false
      }
    )

    // to map the data to a Tuple[(clickid, session_id),1L]
    // and reduce the Tuple to ((category_id,sessionId), sum)
    val countedCIDandSessionRDD: RDD[((String, String), Long)] = filteredRDD.map(
      line => {
        val lineArray: Array[String] = line.split("_")
        ((lineArray(6), lineArray(2)), 1L)
      }
    ).reduceByKey(_ + _)

    val formedCIDandSessionRDD: RDD[(String, (String, Long))] = countedCIDandSessionRDD.map {
      case ((cid, sess), cnt) => {
        (cid, (sess, cnt))
      }
    }

    //group the key of (category_id,(sessionId, sum)),
    val groupedCIDRDD: RDD[(String, Iterable[(String, Long)])] = formedCIDandSessionRDD.groupByKey()

    //sort the List(sessionId, sum) by sum
    println("First 5 users in best10 category: ")
    groupedCIDRDD.mapValues(
      itr => {
        itr.toList.sortBy(_._2)(Ordering.Long.reverse).take(5)
      }
    ).collect().foreach(x => println(s"category_id:${x._1},first 5 session_ids & total clicking number in each session_id: ${x._2}"))

    sc.stop()
  }


  //  define the method(top20Category) to get the best 10 category
  def best10Categories(dataRDD: RDD[String]): Array[String] = {
    /*
        1. parse the data, when click ,then map to a Tuple:  (cid, (1,0,0))
                           when book , then map to a Tuple:  (cid, (0,1,0))
                            when pay , then map to a Tuple:  (cid, (0,0,1))
        2. reduce the result by Key
        3. sort to get best10

         */
    val mappedCIDRDD: RDD[(String, (Int, Int, Int))] = dataRDD.flatMap(
      line => {
        val lineArray: Array[String] = line.split("_")
        if (lineArray(6) != "-1") {
          Array((lineArray(6), (1, 0, 0)))
        } else if (lineArray(8) != "null") {
          lineArray(8).split(",").map(
            id => {
              (id, (0, 1, 0))
            }
          )

        } else if (lineArray(10) != "null") {
          lineArray(10).split(",").map(
            id => {
              (id, (0, 0, 1))
            }
          )

        } else {
          Nil
        }
      }
    )

    //mappedCIDRDD: RDD[(String, (Int, Int, Int))]
    val reducedCidRDD: RDD[(String, (Int, Int, Int))] = mappedCIDRDD.reduceByKey(
      (x, y) => {
        (x._1 + y._1, x._2 + y._2, x._3 + y._3)
      }
    )

    //sort to get best10
    val best10CategoriesArray: Array[String] = reducedCidRDD.sortBy(_._2, ascending = false).take(10).map(_._1)
    best10CategoriesArray
  }

}
