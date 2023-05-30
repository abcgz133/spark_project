package creditcard_sparkProject.Trend_Payment_type_From_Kafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.text.SimpleDateFormat
import java.util.{Date, Properties, Random}
import scala.collection.mutable.ListBuffer

object SparkStreaming_MockData {

  def main(args: Array[String]): Unit = {

    // to generate simulation mock data
    // formate ：timestamp area city user_id ad_id
    // meaning: timeStamp area city user_id, advertisement_id

    // Application => Kafka => SparkStreaming => Analysis
    val prop = new Properties()
    // 添加配置
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop01:9092")
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](prop)
    var time = 0
    while (true) {
      val data: ListBuffer[String] = dataMock()
      data.foreach(
        dataEach => {
          val producerRecord: ProducerRecord[String, String] = new ProducerRecord[String, String]("aiShengYing", dataEach)
          producer.send(producerRecord)
          println("sending to Producer :" + dataEach)


        }
      )
      time += 1
      println(s"time is ${time}. now begin to sleep 5 seconds")
      Thread.sleep(5000)
    }

  }

  def dataMock(): ListBuffer[String] = {
    // dataMock format : System.currentTimeMillis area city uid aid
    val dataList = ListBuffer[String]()
    val transaction_category = ListBuffer[String]("00", "00", "00", "01","01","01","02")
    for (i <- 1 to new Random().nextInt(50)) {
      val transaction_type = transaction_category(new Random().nextInt(7))
      val card_no = "535918008099" +  ((new Random().nextInt(10)).toString  + (new Random().nextInt(10)).toString) + ((new Random().nextInt(10)).toString  + (new Random().nextInt(10)).toString)
      val merchant_id = "10244018398" + ((new Random().nextInt(10)).toString  + (new Random().nextInt(10)).toString) + ((new Random().nextInt(10)).toString  + (new Random().nextInt(10)).toString)
      val sys_time = System.currentTimeMillis
      val newTS = sys_time / 10000 * 10000
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val tran_time_stats = sdf.format(new Date(newTS.toLong))
      val original_sys_time = sdf.format(new Date(sys_time.toLong))
      dataList.append(s"${System.currentTimeMillis()} ${transaction_type} ${card_no} ${merchant_id} ")
    }

    dataList
  }


}
