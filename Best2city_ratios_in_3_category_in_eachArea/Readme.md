Best2city_ratios_in_3_category_in_eachArea.scala

## 1.functions of this system.
the system can list the most 3 popular products and related total clicking number in different areas in China.
The most complicated thing is that the spark SQL can not only list the corresponding city names in each area group, but can also use UDAF method to compute and list the 2 top transaction number ration of each city to the whole region.

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

 



## 3. Code description
### 3.1 file(First2cities_in_3_category_in_eachArea.scala)
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

## 4 the result:
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
|??????|??????_86|      371| ?????? 16%,?????? 15%,3rd...|   1|
|??????|??????_47|      366| ?????? 15%,?????? 15%,3rd...|   2|
|??????|??????_75|      366| ?????? 17%,?????? 15%,3rd...|   2|
|??????|??????_15|      116|        ?????? 54%,?????? 45%|   1|
|??????| ??????_2|      114|        ?????? 53%,?????? 46%|   2|
|??????|??????_22|      113|        ?????? 54%,?????? 45%|   3|
|??????|??????_23|      224| ?????? 29%,?????? 24%,3rd...|   1|
|??????|??????_65|      222| ?????? 27%,?????? 26%,3rd...|   2|
|??????|??????_50|      212| ?????? 27%,?????? 25%,3rd...|   3|
|??????|??????_42|      264| ?????? 25%,?????? 25%,3rd...|   1|
|??????|??????_99|      264| ?????? 24%,?????? 23%,3rd...|   1|
|??????|??????_19|      260| ?????? 23%,?????? 20%,3rd...|   3|
|??????|??????_41|      169|????????? 35%,?????? 34%,3r...|   1|
|??????|??????_91|      165|????????? 35%,?????? 32%,3r...|   2|
|??????|??????_58|      159| ?????? 37%,?????? 32%,3rd...|   3|
|??????|??????_93|      159|????????? 38%,?????? 37%,3r...|   3|
|??????|??????_62|      117|        ?????? 51%,?????? 48%|   1|
|??????| ??????_4|      113|        ?????? 53%,?????? 46%|   2|
|??????|??????_57|      111|        ?????? 54%,?????? 45%|   3|
|??????|??????_29|      111|        ?????? 50%,?????? 49%|   3|
+----+-------+---------+-------------------------+----+
```
