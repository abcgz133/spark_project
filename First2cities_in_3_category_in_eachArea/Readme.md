First2cities_in_3_category_in_eachArea.scala

## 1.functions of this system.
the system can list the most 3 popular products and related total clicking number in different areas in China.
in each area, the system can also respectively list 2 biggest rates and names of the cities in these clicking transactions. 

## 2.Why using the SparkSQL?

###3. the newest user defined aggregation function--Aggregator:
Users can use spark.UDF function to add user-defined functions to implement user-defined functions in SparkSQL.

#### how to extend the Aggregator to implement user-defined function?
An aggregator Class extends the Aggregator. It should firstly define the type of In , Out and Buffer. 
Secondly, it should override these 6 methods:
  zero() : to new a zero value Buffer.
  reduce(): to define how to aggregate an In instance to the Buffer instance.
  merge(): to define how to merge two Buffer instances.
  finish(): to define how to input the Input instance.
  bufferEncoder(): to define how to encode the buffer.
  outputEncoder(): to define how to encode the output. 

 

## 3. Project resource structure


```
E:.                                          
├─.idea                                      
│  └─codeStyles                              
├─datas                                      
├─output
├─spark-receiver
│  └─receivedBlockMetadata
├─src
│  ├─main
│  │  ├─java
│  │  │  └─cn
│  │  │      └─aishengying
│  │  │          ├─createorder
│  │  │          └─util
│  │  ├─resources
│  │  └─scala
│  │      └─sparkProject
│  │          ├─Best10Clicking_Booking_Paying
│  │          ├─BlackList_filter_create
│  │          ├─First2cities_in_3_category_in_eachArea
│  │          ├─First5goodUsers_in_best10Categories_Analysis
│  │          ├─FiveMinuteFromKafkaTrend
│  │          └─PageFlows_covertingRate
│  └─test
│      └─java
└─target
    └─classes
       └─sparkProject
          ├─Best10Clicking_Booking_Paying
          ├─BlackList_filter_create
          ├─First2cities_in_3_category_in_eachArea
          ├─First5goodUsers_in_best10Categories_Analysis
          ├─FiveMinuteFromKafkaTrend
          └─PageFlows_covertingRate
       

```



## 4. Code description
### 4.1 file(First2cities_in_3_category_in_eachArea.scala)
```
  /*
    1. parse the textFile to table user_action, city_info and product_info
    2. select all necessary data that join thove 3 tables(user_action, city_info and product_info) to form a big table(named big_table1).
	3. to count and order the city_rate by extends the UDAF of Aggregator to form the rate of first 2 cities.
    4. to select the areas, the products ,the rank , and the city_rate from the big_table1 group by
    area, product_name to form an another big table(named big_table2)
    5. to select * from big_table2 where the rank <=3 to form an another big table(named big_table3)

      */
```

## 5 the result:
### English version:
```
+----+-------+---------+-------------------------+----+
|area|   NAME|CLICK_CNT|                CITY_RATE|RANK|
+----+-------+---------+-------------------------+----+
|Eastern China|product_86|      371| Shanghai 16%,Hangzhou 15%,3rd...|   1|
|Eastern China|product_47|      366| Hangzhou 15%,QingDao 15%,3rd...|   2|
|Eastern China|product_75|      366| Shanghai 17%,WUXI 15%,3rd...|   2|
|WestNorthChina|product_15|      116|        XIAN 54%,YINCUAN 45%|   1|
|WestNorthChina| product_2|      114|        YINCUAN 53%,XIAN 46%|   2|
|WestNorthChina|product_22|      113|        XIAN 54%,YINCUAN 45%|   3|
|Southern China|product_23|      224| XIMEN 29%,FUZHOU 24%,3rd...|   1|
|Southern China|product_65|      222| SHENZHEN 27%,XIMEN 26%,3rd...|   2|
|Southern China|product_50|      212| FUZHOU 27%,SHENZHEN 25%,3rd...|   3|
|Northern China|product_42|      264| ZHENGZHOU 25%,BAODING 25%,3rd...|   1|
|Northern China|product_99|      264| BEIJING 24%,ZHENGZHOU 23%,3rd...|   1|
|Northern China|product_19|      260| ZHENGZHOU 23%,BAODING 20%,3rd...|   3|
|EastNorth China|product_41|      169|HAERBING 35%,DALIAN 34%,3r...|   1|
|EastNorth China|product_91|      165|HAERBING 35%,DALIAN 32%,3r...|   2|
|EastNorth China|product_58|      159| SHENGYANG 37%,DALIAN 32%,3rd...|   3|
|EastNorth China|product_93|      159|HAERBING 38%,DALIAN 37%,3r...|   3|
|Middle China|product_62|      117|        WUHAN 51%,CHANGSHA 48%|   1|
|Middle China| product_4|      113|        CHANGSHA 53%,WUHAN 46%|   2|
|Middle China|product_57|      111|        WUHAN 54%,CHANGSHA 45%|   3|
|Middle China|product_29|      111|        WUHAN 50%,CHANGSHA 49%|   3|
+----+-------+---------+-------------------------+----+

```

### Original Chinese Version:
```
+----+-------+---------+-------------------------+----+
|area|   NAME|CLICK_CNT|                CITY_RATE|RANK|
+----+-------+---------+-------------------------+----+
|华东|商品_86|      371| 上海 16%,杭州 15%,3rd...|   1|
|华东|商品_47|      366| 杭州 15%,青岛 15%,3rd...|   2|
|华东|商品_75|      366| 上海 17%,无锡 15%,3rd...|   2|
|西北|商品_15|      116|        西安 54%,银川 45%|   1|
|西北| 商品_2|      114|        银川 53%,西安 46%|   2|
|西北|商品_22|      113|        西安 54%,银川 45%|   3|
|华南|商品_23|      224| 厦门 29%,福州 24%,3rd...|   1|
|华南|商品_65|      222| 深圳 27%,厦门 26%,3rd...|   2|
|华南|商品_50|      212| 福州 27%,深圳 25%,3rd...|   3|
|华北|商品_42|      264| 郑州 25%,保定 25%,3rd...|   1|
|华北|商品_99|      264| 北京 24%,郑州 23%,3rd...|   1|
|华北|商品_19|      260| 郑州 23%,保定 20%,3rd...|   3|
|东北|商品_41|      169|哈尔滨 35%,大连 34%,3r...|   1|
|东北|商品_91|      165|哈尔滨 35%,大连 32%,3r...|   2|
|东北|商品_58|      159| 沈阳 37%,大连 32%,3rd...|   3|
|东北|商品_93|      159|哈尔滨 38%,大连 37%,3r...|   3|
|华中|商品_62|      117|        武汉 51%,长沙 48%|   1|
|华中| 商品_4|      113|        长沙 53%,武汉 46%|   2|
|华中|商品_57|      111|        武汉 54%,长沙 45%|   3|
|华中|商品_29|      111|        武汉 50%,长沙 49%|   3|
+----+-------+---------+-------------------------+----+
```
