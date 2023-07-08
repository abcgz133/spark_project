package creditcard_sparkProject.the3rd_card_type_Best_market_share_ration_in_Branches

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object card_type_Best_market_share_ration_in_Branches {
  /*
    1. parse the textFile to tables:transaction_Item, city_branch and card_type_info
    2. select all necessary data that join these 3 tables together to form a big table(named: items_cardtype_cityBranch).
  	3. to compute and order the Market Share Proportion and Ranking by extending the UDAF(User Define Aggregation Function) to form the market share ration of first 2 city branches.
    4. to group the area ,card_type_name and then select area, card_type, the number of total transactions ,the rank , and the Market Share Proportion  from the new big table to form an another big table(named: grouped_items_cardtype_cityBranch)
    5. to select * from grouped_items_cardtype_cityBranch where the rank <=3 to form an another big table(named: ranked_items_cardtype_cityBranch)

      */

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local[*]").setAppName("card_type_best_market_share_ratio")
    val sc = new SparkContext(sparConf)
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .getOrCreate()

    //parse the textFile to table transaction_Item, city_branch and card_type_info
    val dataRDD: RDD[String] = sc.textFile("data/creditcard_transaction_items.txt")
    val transactionItemRDD: RDD[TransactionItems] = dataRDD.map(
      line => {
        val lineArray: Array[String] = line.split("_")
        TransactionItems(
          lineArray(0),
          lineArray(1).toLong,
          lineArray(2),
          lineArray(3).toLong,
          lineArray(4),
          lineArray(5),
          lineArray(6).toLong,
          lineArray(7).toLong,
          lineArray(8),
          lineArray(9),
          lineArray(10),
          lineArray(11),
          lineArray(12).toLong
        )
      }
    )

    transactionItemRDD.cache()
    import spark.implicits._
    transactionItemRDD.toDF().createOrReplaceTempView("transaction_items")
    spark.sql("select * from transaction_items").show()

    val cityDataRDD: RDD[String] = sc.textFile("data/creditcard_city_branch.txt")
    val cityINFORDD: RDD[cityBranch] = cityDataRDD.map(
      line => {
        val citylineArray: Array[String] = line.split("\t")
        cityBranch(
          citylineArray(0).toLong,
          citylineArray(1),
          citylineArray(2)
        )
      }
    )
    cityDataRDD.cache()

    cityINFORDD.toDF().createOrReplaceTempView("city_branch")
    spark.sql("select * from city_branch").show()

    val productDataRDD: RDD[String] = sc.textFile("data/creditcard_card_type_info.txt")
    val productRDD: RDD[ProductInfo] = productDataRDD.map(
      line => {
        val productArray: Array[String] = line.split("\t")
        ProductInfo(
          productArray(0).toLong,
          productArray(1),
          productArray(2)
        )
      }
    )
    productRDD.cache()
    productRDD.toDF().createOrReplaceTempView("card_type_info")
    spark.sql("select * from card_type_info").show()

    //select all necessary data from a big table(named: items_cardtype_cityBranch) that join these 3 tables.
    spark.sql(
      """
        |select *,area, city_name, product_name  from transaction_items u
        |join city_branch c on c.city_id = u.city_id
        |join card_type_info p on u.consume_card_type_id = p.card_type_id
        |where u.transaction_category_id != -1
        |""".stripMargin).createOrReplaceTempView("items_cardtype_cityBranch")

    //select the areas, the card_types ,the rank , and the Market Share Proportion and Ranking from the items_cardtype_cityBranch grouping by
    // area, card_type_name to form an another big table(named :grouped_items_cardtype_cityBranch)
    spark.udf.register("cityRate", functions.udaf(new CityRateUDAF()))
    spark.sql(
      """
        |select area, product_name as Card_Type_NAME,
        |count(*) as Transaction_CNT,
        |cityRate(city_name) as Market_Share_Proportion
        |from items_cardtype_cityBranch
        |group by area, product_name
        |""".stripMargin).createOrReplaceTempView("grouped_items_cardtype_cityBranch")

    //to select * from grouped_items_cardtype_cityBranch where the rank <=3 to form an another big table(named ranked_items_cardtype_cityBranch)
    spark.sql(
      """
        |select *, rank() over(partition by area order by Transaction_CNT desc ) as RANK from grouped_items_cardtype_cityBranch
        |""".stripMargin).createOrReplaceTempView("ranked_items_cardtype_cityBranch")

    //to select * from grouped_items_cardtype_cityBranch where the rank <=3 to form an another big table(named ranked_items_cardtype_cityBranch)
    spark.sql(
      """
        |select * from ranked_items_cardtype_cityBranch where RANK <=3
        |""".stripMargin).show(truncate = false)

    sc.stop()
    spark.close()
  }

  case class TransactionItems(
                              date: String, //the date of user click
                              user_id: Long, //user id
                              session_id: String, //Session ID
                              page_id: Long, //PageID
                              action_time: String, //action time
                              search_keyword: String, //key word in search
                              transaction_category_id: Long, // category ID of selling goods
                              consume_card_type_id: Long, // product ID
                              order_category_ids: String, //category ids in one ordering
                              order_product_ids: String, // product_ids in one ordering
                              pay_category_ids: String, // category ids in one paying
                              pay_product_ids: String, // product_ids in one paying
                              city_id: Long    // city id
                            )


  case class cityBranch(
                       city_id: Long,
                       city_name: String,
                       area: String
                     )

  case class ProductInfo(
                          card_type_id: Long,
                          product_name: String,
                          extend_info: String
                        )


  case class Buffer(var total_cnt: Long, var city_map: mutable.Map[String, Long])




  class CityRateUDAF extends Aggregator[String, Buffer, String] {
    override def zero: Buffer = {
      Buffer(0L, mutable.Map[String, Long]())
    }

    override def reduce(buffer: Buffer, city: String): Buffer = {
      buffer.total_cnt += 1
      val map: mutable.Map[String, Long] = buffer.city_map
      val city_new_count: Long = map.getOrElse(city, 0L) + 1
      map.update(city, city_new_count)
      buffer.city_map = map
      buffer

    }

    override def merge(buffer1: Buffer, buffer2: Buffer): Buffer = {
      buffer1.total_cnt += buffer2.total_cnt
      val map1: mutable.Map[String, Long] = buffer1.city_map
      val map2: mutable.Map[String, Long] = buffer2.city_map
      map2.foreach {
        case (city, cnt) => {
          val newCnt: Long = map1.getOrElse(city, 0L) + cnt
          map1.update(city, newCnt)
        }
      }
      buffer1.city_map = map1
      buffer1
    }

    override def finish(reduction: Buffer): String = {
      /*
      1. sort the map in reduction ordering with the count of city
      2. count the rate of each city by dividing by the total_cnt
      3. take just 2 biggest cities, the rest will be (100%-the 2 biggest cities)
      4. new a ListBuffer to be appended and at last to be formed to a String
      */

      var stringList = ListBuffer[String]()
      val map: mutable.Map[String, Long] = reduction.city_map
      val sortedCityList: List[(String, Long)] = map.toList.sortWith(
        (left, right) => {
          left._2 > right._2
        }
      ).take(2)

      //count the rate of each city by dividing the total_cnt
      // It seems that the variable(sum_of_two) can be used here instead of th accumulator
      // it is because it is in the final finish operation. it is only processed in the Drive.
      var sum_of_two = 0L
      sortedCityList.foreach {
        case (city, cnt) => {
          val rate = cnt * 100 / reduction.total_cnt
          stringList.append(s"${city} :${rate}% ")
          sum_of_two += rate
        }
      }

      //the rest will be (100%-the 2 biggest cities)
      if (map.size > 2) {
        stringList.append(s"3rd: ${100 - sum_of_two}%")
      }
      stringList.mkString(",")

    }

    override def bufferEncoder: Encoder[Buffer] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }
}
