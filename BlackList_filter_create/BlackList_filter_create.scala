package sparkProject.BlackList_filter_create

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.JDBCUtil

import java.sql.{PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

object BlackList_filter_create {
  /*
    1. receive data from Kafka,
    2. transform the data to DStream and parse the data to a case class
    3. select user_id from black_list, then filter the data if user_id is not in  the black_list
    4. map the filtered data to ((day, user_id, ad_id),1)  and then reduce them to count the total number
     of clicks to each advertise_id by each user_id in each day.
    5. if counted_total_number >20 then insert or update the blacklist.
    6. if counted_total_number <=20, then update or insert into the user_ad_count ,
    if the updated counted_total_number >20, then insert or update the user_id into  black_list

     */

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop01:9092,hadoop01:9092,hadoop01:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "aiShengYing",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("aiShengYing"), kafkaPara)
    )

    val dataAdDstream: DStream[AdClickData] = kafkaDataDS.map(
      kafkadata => {
        val dataArray: Array[String] = kafkadata.value().split(" ")
        AdClickData(
          dataArray(0),
          dataArray(1),
          dataArray(2),
          dataArray(3),
          dataArray(4)
        )
      }
    )
    //filter the black_list from the data
    val reducedGroupedDstream: DStream[((String, String, String), Int)] = dataAdDstream.transform(
      rdd => {
        val conn = JDBCUtil.getConnection
        val preparedStatement: PreparedStatement = conn.prepareStatement("select user_id from black_list")
        val resultSet: ResultSet = preparedStatement.executeQuery()
        var black_list = ListBuffer[String]()
        while (resultSet.next()) {
          black_list.append(resultSet.getString(1))
        }
        resultSet.close()
        preparedStatement.close()
        conn.close()

        val filteredRDD: RDD[AdClickData] = rdd.filter(
          data => {
            !black_list.contains(data.user_id)
          }
        )

        filteredRDD.map(
          data => {
            val sdf = new SimpleDateFormat("yyyy-MM-dd")
            val day1: String = sdf.format(new Date(data.day.toLong))
            ((day1, data.user_id, data.ad_id), 1)
          }
        ).reduceByKey(_ + _)

      }
    )

    reducedGroupedDstream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          data => data.foreach {

            case ((day, user_id, ad_id), count) => {
              println(s"day: ${day} user_id:${user_id} ad_id:${ad_id} count:${count} ")
              // if count <20, insert or update the user_ad_count
              // then if the new counted > 20 then insert or update into
              // the black_list
              if (count < 20) {
                val conn = JDBCUtil.getConnection
                val sql1 =
                  """
                    |insert into user_ad_count (day, user_id, ad_id, count) values (?,?,?,?)
                    |on duplicate key
                    |update count = count + ?
                    |""".stripMargin
                JDBCUtil.executeUpdate(conn, sql1, Array(day, user_id, ad_id, count, count))

                val sql2 =
                  """
                    |select user_id from user_ad_count
                    |where day = ?
                    |and user_id = ?
                    |and ad_id = ?
                    |and count >= 20
                    |""".stripMargin
                val flag: Boolean = JDBCUtil.isExist(conn, sql2, Array(day, user_id, ad_id))

                if (flag) {
                  val sql3 =
                    """
                      |insert into black_list (user_id) values (?)
                      |on duplicate key
                      |update user_id = ?
                      |""".stripMargin
                  JDBCUtil.executeUpdate(conn, sql3, Array(user_id, user_id))

                }
                conn.close()
              } else {
                // count >= 20
                val conn = JDBCUtil.getConnection
                val sql4 =
                  """
                    |insert into black_list (user_id) values (?)
                    |on duplicate key
                    |update user_id = ?
                    |""".stripMargin
                JDBCUtil.executeUpdate(conn, sql4, Array(user_id, user_id))
                conn.close()
              }


            }

          }
        )
      }
    )

    //reducedGroupedDstream: DStream[((String, String, String), Int)]



    ssc.start()
    ssc.awaitTermination()
  }

  // data of ad clicking
  case class AdClickData(day: String, area: String, city: String, user_id: String, ad_id: String)

}
