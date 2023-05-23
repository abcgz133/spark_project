package creditcard_sparkProject.fiveMinuteFromKafkaTrend

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FiveMinuteFromKafkaTrend {
    /*
    1. receive the data from the Producer and parse the data to a Case Class AdClickData.
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

        //adClickData :DStream[AdClickData]
        val adClickData = kafkaDataDS.map(
            kafkaData => {
                val data = kafkaData.value()
                val datas = data.split(" ")
                AdClickData(datas(0),datas(1),datas(2),datas(3),datas(4))
            }
        )


        //adClickData :DStream[AdClickData]
        //reduceDS :DStream[(Long, Int)]
        val reduceDS = adClickData.map(
            data => {
                val ts = data.ts.toLong
                val newTS = ts / 10000 * 10000
                ( newTS, 1 )
            }
        ).reduceByKeyAndWindow((x:Int,y:Int)=>{x+y}, Seconds(60 *5 ), Seconds(30))

        reduceDS.print()


        ssc.start()
        ssc.awaitTermination()
    }
    // Advertising clicking Data
    case class AdClickData( ts:String, area:String, city:String, user:String, ad:String )

}
