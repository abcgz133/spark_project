package creditcard_sparkProject.Transactions_Real_Time_Risk_Monitoring_and_processing

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.{Properties, Random}
import scala.collection.mutable.ListBuffer

object SparkStreaming_MockData {

  def main(args: Array[String]): Unit = {
    /*
    1. to Generate simulation Mock data
     formate: timestamp area city user_id ad_id
     meaning: timestamp area city user_id,advertisement_id
    2. to produce the data to Producer of Kafka, the topic is "aiShengYing"
     */


    // Application => Kafka => SparkStreaming => Analysis
    val prop = new Properties()

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
      println(s"time is ${time}. now begin to sleep 3 seconds")
      Thread.sleep(3000)
    }

  }

  def dataMock(): ListBuffer[String] = {
    // dataMock format : System.currentTimeMillis area city card aid
    val transaction_type_list = ListBuffer[String]("00", "01", "02","03")

    val dataList = ListBuffer[String]()
    for (i <- 1 to new Random().nextInt(50)) {
      val amount = (new Random().nextInt(8)) * 10000
      val transaction_type = transaction_type_list(new Random().nextInt(4))
      val card = "535918008008120" + (new Random().nextInt(10) )
      val aid = "10244018998105" + (new Random().nextInt(6) + 1)
      dataList.append(s"${System.currentTimeMillis()} ${amount} ${transaction_type} ${card} ${aid}")
    }

    dataList
  }


}
