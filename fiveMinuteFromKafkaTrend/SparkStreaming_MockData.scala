package creditcard_sparkProject.fiveMinuteFromKafkaTrend

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.{Properties, Random}
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
    val areaList: ListBuffer[String] = ListBuffer[String]("SouthernChina", "EasternChina", "WesternChina")
    val cityList = ListBuffer[String]("Beijing", "Shanghai", "Shenzhen", "Guangzhou")

    val dataList = ListBuffer[String]()
    for (i <- 1 to new Random().nextInt(50)) {
      val area = areaList(new Random().nextInt(3))
      val city = cityList(new Random().nextInt(4))
      val uid = new Random().nextInt(6) + 1
      val aid = new Random().nextInt(6) + 1
      dataList.append(s"${System.currentTimeMillis()} ${area} ${city} ${uid} ${aid}")
    }

    dataList
  }


}
