package creditcard_sparkProject.Transactions_Real_Time_Risk_Monitoring_and_processing

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

object Transactions_Real_Time_Risk_Monitoring_and_processing {
  /*
    1. receive data from Kafka,
    2. transform the data to DStream and parse the data to a case class
    3. select card from black_list, then filter the data if card is not in the black_list and the amount >=20,000 and the transaction_typs is "00"(means: a consumption type transaction)
    4. map the filtered data to ((day, card, merchant_id),1)  and then reduce them to count the total number
     of clicks to each advertise_id by each card in each day.
    5. if counted_total_number >20 then insert or update the card into the blacklist.
    6. if counted_total_number <=20, then update or insert into the historical_card_merchant_counted ,
    if finding the updated total_number in the history_user_ad_count >20, then insert or update the card into  black_list

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

    val dataAdDstream: DStream[TransactionData] = kafkaDataDS.map(
      kafkadata => {
        val dataArray: Array[String] = kafkadata.value().split(" ")
        TransactionData(
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
        val preparedStatement: PreparedStatement = conn.prepareStatement("select card from black_list")
        val resultSet: ResultSet = preparedStatement.executeQuery()
        var black_list = ListBuffer[String]()
        while (resultSet.next()) {
          black_list.append(resultSet.getString(1))
        }
        resultSet.close()
        preparedStatement.close()
        conn.close()

        val filteredRDD: RDD[TransactionData] = rdd.filter(
          data => {
            (!black_list.contains(data.card) )&&  ((data.amount).toInt >= 20000 && (data.transaction_type ).equals("00"))
          }
        )

        filteredRDD.foreach( x=>println("processing data : " + x))

        filteredRDD.map(
          data => {
            val sdf = new SimpleDateFormat("yyyy-MM-dd")
            val day1: String = sdf.format(new Date(data.day.toLong))
            ((day1, data.card, data.merchant_id), 1)
          }
        ).reduceByKey(_ + _)

      }
    )

    reducedGroupedDstream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          data => data.foreach {

            case ((day, card, merchant_id), counted_total_number) => {
              println(s"day: ${day} card:${card} merchant_id:${merchant_id} counted_total_number:${counted_total_number} ")
              // if counted_total_number <20, insert or update the historical_card_merchant_counted
              // then if the new counted > 20 then insert or update into
              // the black_list
              if (counted_total_number < 20) {
                val conn = JDBCUtil.getConnection
                val sql1 =
                  """
                    |insert into historical_card_merchant_counted (day, card, merchant_id, counted_total_number) values (?,?,?,?)
                    |on duplicate key
                    |update counted_total_number = counted_total_number + ?
                    |""".stripMargin
                JDBCUtil.executeUpdate(conn, sql1, Array(day, card, merchant_id, counted_total_number, counted_total_number))

                val sql2 =
                  """
                    |select card from historical_card_merchant_counted
                    |where day = ?
                    |and card = ?
                    |and merchant_id = ?
                    |and counted_total_number >= 20
                    |""".stripMargin
                val flag: Boolean = JDBCUtil.isExist(conn, sql2, Array(day, card, merchant_id))

                if (flag) {
                  val sql3 =
                    """
                      |insert into black_list (card) values (?)
                      |on duplicate key
                      |update card = ?
                      |""".stripMargin
                  JDBCUtil.executeUpdate(conn, sql3, Array(card, card))

                }
                conn.close()
              } else {
                // counted_total_number >= 20
                val conn = JDBCUtil.getConnection
                val sql4 =
                  """
                    |insert into black_list (card) values (?)
                    |on duplicate key
                    |update card = ?
                    |""".stripMargin
                JDBCUtil.executeUpdate(conn, sql4, Array(card, card))
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
  case class TransactionData(day: String, amount: String, transaction_type: String, card: String, merchant_id: String)

}
