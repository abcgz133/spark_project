package sparkProject.the1st_Trend_Payment_type_From_Kafka

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.JDBCUtil

import java.text.SimpleDateFormat
import java.util.Date

object TransactionTypesFromKafkaTrend {
    /*
    1. receive the data from the Producer and parse the data to a Case Class Transaction_Item.
    2. Classify the time into the nearest part(Divide one minute into six parts).
    3. map this newTime to (newTime,1)
    4. reduceByKeyAndWindow and get the count of (newTime,1) in 5 minutes wide window.
     */
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(5))

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

        //Transaction_Item :DStream[Transaction_Item]
        //val Transaction_Item_data =
        val Transaction_Item_data: DStream[Transaction_Item] = kafkaDataDS.map(
            kafkaData => {
                val data = kafkaData.value()
                val datas = data.split(" ")
                Transaction_Item(datas(0), datas(1), datas(2), datas(3))
            }
        )


        //Transaction_Item :DStream[Transaction_Item]
        //reduceDS :DStream[(Long, Int)]
        val reduceDS: DStream[((String, String), Int)] = Transaction_Item_data.map(
            data => {
                val ts = data.ts.toLong
                val newTS = ts / 10000 * 10000
                val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                val dayAndtime = sdf.format(new Date(newTS.toLong))
                (( dayAndtime, data.transaction_type), 1 )
            }
        ).reduceByKeyAndWindow((x:Int,y:Int)=>{x+y}, Seconds(30 ), Seconds(30))

        reduceDS.print()

        //
        reduceDS.foreachRDD(
            (rdd: RDD[((String, String), Int)]) => {


                rdd.foreachPartition(

                    iterator=> {
                        val conn = JDBCUtil.getConnection
                        iterator.foreach(
                            (x: ((String, String), Int)) => {
                                println("the iterator is " + x)
                                val sql1 =
                                    """
                                      |insert into transaction_item (date_kafka, transaction_type, sub_total_number_consume, time_kafka) values (?, ?, ?,? )
                                      |""".stripMargin
                                JDBCUtil.executeUpdate(conn, sql1, Array(x._1._1.substring(0,10), x._1._2, x._2,x._1._1.substring(11,19) ))
                                val sql2 =
                                    """
                                      |insert into total_transactions( day, total_item_consume , transaction_type) values(?,?,?)
                                      |on duplicate key
                                      |update total_item_consume = total_item_consume + ?
                                      |""".stripMargin
                                //x._1._1.substring(0,9)
                                JDBCUtil.executeUpdate(conn, sql2, Array(x._1._1.substring(0,10), x._2, x._1._2, x._2 ))
                            }
                        )
                        conn.close()
                    }

                )
            }
        )



        //

        ssc.start()
        ssc.awaitTermination()
    }
    // Advertising clicking Data
    case class Transaction_Item( ts:String, transaction_type:String, card_no:String, merchant_id:String )

}
