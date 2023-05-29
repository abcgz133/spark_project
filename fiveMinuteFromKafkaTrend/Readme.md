FiveMinuteFromKafkaTrend.scala

# Using the SparkStreaming dynamically analyze the flowing data from Kafka within 5 minutes in each 10 seconds.


## 1.functions of this system.
the system includes these functions:
1. parse the data from Kafka to DStream.
2. because only showing the data in the 0,10,20,30,40,50 second in one minute, so Classify the time of the data into the nearest part(Divide one minute into six parts).
3. count the total number of clicking within 5 minutes in each 10 seconds.(这里没有说清楚滑窗的概念！)

## 2.Why Spark and Kafka? 
### the advantages of Spark：
1. Spark supports streaming computing framework, high throughput and fault-tolerant processing.
2. Spark supports cluster manager, which can efficiently scale computing from one to thousands of nodes.
3. Comparing Hadoop, which saves processing data on disk, Spark saves data in memory when processing data. Thus, the computational efficiency has beengreatly improved.

### the advantages of Kafka：
Kafka is a distributed subscribe message system with high throughput . Based on zookeeper, it has important functions in real-time computing system.
##### 
## 3. flowchart of this project
```mermaid
flowchart LR
    A(Simulation System)-->| MockData| B(Kafka) --> C(Spark Streaming)
    
```

## 4. System construction:
### Spark high available cluster:
Hadoop: version 2.7.4. It is responsible for the data storage and management.

Spark: version 2.3.2. It is responsible for computing framework.

zookeeper：version 3.4.10

Linux: version CentOS_6.7

JDK: version 1.8

### kafka 
version: 2.11-2.000. 


## 5. Code description
### 5.1 generating the simulation mock data and sending the data to Kafka
(SparkStreaming_MockData.scala)
```
/*
1. to Generate simulation Mock data
     formate: timestamp area city user_id ad_id
     meaning: timestamp area city user_id,advertisement_id
2. to produce the data to Producer of Kafka, the topic is "aiShengYing"
*/
```
in the random method above, system uses the Random to create the clicking data.
It includes the timeStamp, the area name , the city name, the user_id and advertisement_id.


### 5.2 SparkStreaming receives data in Kafka consumer and dynamically analyzes the data in real time
(FiveMinuteFromKafkaTrend.scala)

```
  /*
    1. receive the data from the Producer and parse the data to a Case Class AdClickData.
    2. Classify the time into the nearest part(Divide one minute into six parts).
    3. map this newTime to (newTime,1)
    4. reduceByKeyAndWindow and get the count of (newTime,1) in 5 minutes wide window.
     */
```

### 5.3 the simulation Mock data is like these:

```
sending to Producer :1662360633899 EasternChina Shenzhen 3 1
sending to Producer :1662360633899 EasternChina Shanghai 4 1
sending to Producer :1662360633899 SouthernChina Shanghai 6 2
sending to Producer :1662360633899 SouthernChina Beijing 5 5
sending to Producer :1662360633899 SouthernChina Guangzhou 1 3
sending to Producer :1662360633899 SouthernChina Beijing 3 2
sending to Producer :1662360633899 SouthernChina Shanghai 1 6
sending to Producer :1662360633899 SouthernChina Beijing 1 5
sending to Producer :1662360633899 SouthernChina Guangzhou 5 1
sending to Producer :1662360633899 EasternChina Beijing 1 3
sending to Producer :1662360633899 WesternChina Shenzhen 5 5
sending to Producer :1662360633899 WesternChina Guangzhou 3 1
sending to Producer :1662360633899 SouthernChina Guangzhou 6 5
sending to Producer :1662360633899 SouthernChina Shenzhen 3 4
sending to Producer :1662360633899 EasternChina Shanghai 6 5
sending to Producer :1662360633899 SouthernChina Shanghai 4 4
```
### 5.4 the result is like these:

```
-------------------------------------------
Time: 1662360345000 ms
-------------------------------------------
(1662360320000,73)
(1662360290000,39)
(1662360300000,56)
(1662360330000,39)
(1662360340000,44)
(1662360280000,47)
(1662360310000,41)

-------------------------------------------
Time: 1662360375000 ms
-------------------------------------------
(1662360320000,73)
(1662360370000,1)
(1662360290000,39)
(1662360300000,56)
(1662360330000,39)
(1662360340000,91)
(1662360350000,32)
(1662360360000,55)
(1662360280000,47)
(1662360310000,41)

-------------------------------------------
Time: 1662360405000 ms
-------------------------------------------
(1662360320000,73)
(1662360370000,13)
(1662360290000,39)
(1662360300000,56)
(1662360400000,14)
(1662360330000,39)
(1662360340000,91)
(1662360350000,32)
(1662360360000,55)
(1662360390000,86)
```

