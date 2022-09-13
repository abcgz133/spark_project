package sparkProject.Best2city_ratios_in_3_category_in_eachArea

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Best2city_ratios_in_3_category_in_eachArea {
  /*
    1. parse the textFile to table user_action, city_info and product_info
    2. select all necessary data that join thove 3 tables(user_action, city_info and product_info) to form a big table(named big_table1).
  	3. to count and order the city_rate by extends the UDAF of Aggregator to form the rate of first 2 cities.
    4. to select the areas, the products ,the rank , and the city_rate from the big_table1 group by
    area, product_name to form an another big table(named big_table2)
    5. to select * from big_table2 where the rank <=3 to form an another big table(named big_table3)

      */

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local[*]").setAppName("first2cities_in_3_catagories")
    val sc = new SparkContext(sparConf)
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .getOrCreate()

    //parse the textFile to table user_action, city_info and product_info
    val dataRDD: RDD[String] = sc.textFile("data/user_visit_action.txt")
    val userActionRDD: RDD[UserVisitAction] = dataRDD.map(
      line => {
        val lineArray: Array[String] = line.split("_")
        UserVisitAction(
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

    userActionRDD.cache()
    import spark.implicits._
    userActionRDD.toDF().createOrReplaceTempView("user_visit_action")
    spark.sql("select * from user_visit_action").show()

    val cityDataRDD: RDD[String] = sc.textFile("data/city_info.txt")
    val cityINFORDD: RDD[CityInfo] = cityDataRDD.map(
      line => {
        val citylineArray: Array[String] = line.split("\t")
        CityInfo(
          citylineArray(0).toLong,
          citylineArray(1),
          citylineArray(2)
        )
      }
    )
    cityDataRDD.cache()

    cityINFORDD.toDF().createOrReplaceTempView("city_info")
    spark.sql("select * from city_info").show()

    val productDataRDD: RDD[String] = sc.textFile("data/product_info.txt")
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
    productRDD.toDF().createOrReplaceTempView("product_info")
    spark.sql("select * from product_info").show()

    //select all necessary data from a big table(named big_table1) that join these 3 tables.
    spark.sql(
      """
        |select *,area, city_name, product_name  from user_visit_action u
        |join city_info c on c.city_id = u.city_id
        |join product_info p on u.click_product_id = p.product_id
        |where u.click_category_id != -1
        |""".stripMargin).createOrReplaceTempView("big_table1")

    //select the areas, the products ,the rank , and the city_rate from the big_table1 group by
    // area, product_name to form an another big table(named big_table2)
    spark.udf.register("cityRate", functions.udaf(new CityRateUDAF()))
    spark.sql(
      """
        |select area, product_name as NAME,
        |count(*) as CLICK_CNT,
        |cityRate(city_name) as CITY_RATE
        |from big_table1
        |group by area, product_name
        |""".stripMargin).createOrReplaceTempView("big_table2")

    //to select * from big_table2 where the rank <=3 to form an another big table(named big_table3)
    spark.sql(
      """
        |select *, rank() over(partition by area order by CLICK_CNT desc ) as RANK from big_table2
        |""".stripMargin).createOrReplaceTempView("big_table3")

    //to select * from big_table2 where the rank <=3 to form an another big table(named big_table3)
    spark.sql(
      """
        |select * from big_table3 where RANK <=3
        |""".stripMargin).show()

    sc.stop()
    spark.close()
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


  case class CityInfo(
                       city_id: Long,
                       city_name: String,
                       area: String
                     )

  case class ProductInfo(
                          product_id: Long,
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

      //count the rate of each city by dividing by the total_cnt
      var sum_of_two = 0L
      sortedCityList.foreach {
        case (city, cnt) => {
          val rate = cnt * 100 / reduction.total_cnt
          stringList.append(s"${city} ${rate}%")
          sum_of_two += rate
        }
      }

      //the rest will be (100%-the 2 biggest cities)
      if (map.size > 2) {
        stringList.append(s"3rd ${100 - sum_of_two}%")
      }
      stringList.mkString(",")

    }

    override def bufferEncoder: Encoder[Buffer] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }
}
