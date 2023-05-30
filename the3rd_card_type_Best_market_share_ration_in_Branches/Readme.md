card_type_Best_market_share_ration_in_Branches.scala

## 1.functions of this system.
Listing the top 3 total transaction number of credit card types and ranking in each Chinese areas, importantly, also listing the market share proportion and ranking of these card types in different branches under respectively areas.
The most complicated thing is that the spark SQL can not only list the corresponding city branches name in each area group, but can also use UDAF method to compute the market share proportion and ranking.

## 2.Why using the SparkSQL?

###3. the newest user defined aggregation function--Aggregator:
Technically, programmers can use spark.UDF function to add user-defined Aggregation function to implement user-defined function in SparkSQL.

#### how to extend the Aggregator to implement user-defined function?
firstly, defining an aggregator Class to extend the Aggregator. It should define the type of In , Out and Buffer.   
Secondly, it should override these 6 methods:  
  zero() : to create or new a zero value Buffer.  
  reduce(): to define how to aggregate an In instance to the Buffer instance.  
  merge(): to define how to merge two Buffer instances.  
  finish(): to define how to output the Out instance.  
  bufferEncoder(): to define how to encode the buffer.  
  outputEncoder(): to define how to encode the output.   

 



## 3. Code description
### 3.1 file(card_type_Best_market_share_ration_in_Branches.scala)
```
 /*
    1. parse the textFile to tables:transaction_Item, city_branch and card_type_info
    2. select all necessary data that join these 3 tables together to form a big table(named: items_cardtype_cityBranch).
  	3. to compute and order the Market Share Proportion and Ranking by extending the UDAF(User Define Aggregation Function) to form the market share ration of first 2 city branches.
    4. to group the area ,card_type_name and then select area, card_type, the number of total transactions ,the rank , and the Market Share Proportion  from the new big table to form an another big table(named: grouped_items_cardtype_cityBranch)
    5. to select * from grouped_items_cardtype_cityBranch where the rank <=3 to form an another big table(named: ranked_items_cardtype_cityBranch)

      */
```

## 4 the result:
```
+---------------+------------------------------------------+---------------+----------------------------------------------------+----+
|area           |Card_Type_NAME                            |Transaction_CNT|Market_Share_Proportion                             |RANK|
+---------------+------------------------------------------+---------------+----------------------------------------------------+----+
|East China     |Standardized VISA cards                   |371            |ShangHai Branch :16% ,HangZhou Branch :15% ,3rd: 69%|1   |
|East China     |Centurion VISA cards                      |366            |ShangHai Branch :17% ,WuXi Branch :15% ,3rd: 68%    |2   |
|East China     |High-class MASTER cards                   |366            |HangZhou Branch :15% ,QingDao Branch :15% ,3rd: 70% |2   |
|NorthEast China|Chi-bi Maruko co-branded MASTER cards     |169            |HarBin Branch :35% ,DaNian Branch :34% ,3rd: 31%    |1   |
|NorthEast China|GQ co-branded VISA cards                  |165            |HarBin Branch :35% ,DaNian Branch :32% ,3rd: 33%    |2   |
|NorthEast China|AngryBirds co-branded MASTER cards        |159            |HarBin Branch :38% ,DaNian Branch :37% ,3rd: 25%    |3   |
|NorthEast China|NARUTO co-branded VISA cards              |159            |ShenYang Branch :37% ,DaNian Branch :32% ,3rd: 31%  |3   |
|North China    |Corporate MASTER cards                    |264            |BaoDing Branch :25% ,ZhengZhou Branch :25% ,3rd: 50%|1   |
|North China    |Tencent co-branded VISA cards             |264            |BeiJing Branch :24% ,ZhengZhou Branch :23% ,3rd: 53%|1   |
|North China    |IQiyi co-branded VISA cards               |260            |ZhengZhou Branch :23% ,BaoDing Branch :20% ,3rd: 57%|3   |
|SouthWest China|Lucky Carp MASTER cards                   |176            |ChengDu Branch :35% ,GuiYang Branch :35% ,3rd: 30%  |1   |
|SouthWest China|Standard car owner VISA cards             |169            |GuiYang Branch :37% ,ChengDu Branch :34% ,3rd: 29%  |2   |
|SouthWest China|Starlight VISA cards                      |163            |Chongqing Branch :39% ,ChengDu Branch :30% ,3rd: 31%|3   |
|South China    |Social network featured MASTER cards      |224            |XiaMen Branch :29% ,FuZhou Branch :24% ,3rd: 47%    |1   |
|South China    |Game-featured MASTER cards                |222            |ShenZhen Branch :27% ,XiaMen Branch :26% ,3rd: 47%  |2   |
|South China    |Disney co-branded VISA cards              |212            |FuZhou Branch :27% ,ShenZhen Branch :25% ,3rd: 48%  |3   |
|NorthWest China|Specialized MASTER cards                  |116            |Xi'An Branch :54% ,YinChuan Branch :45%             |1   |
|NorthWest China|Fashion-featured VISA cards               |114            |YinChuan Branch :53% ,Xi'An Branch :46%             |2   |
|NorthWest China|Cartoon-featured MASTER cards             |113            |Xi'An Branch :54% ,YinChuan Branch :45%             |3   |
|Central China  |THE LOST CANVAS co-branded VISA cards     |117            |Wuhan Branch :51% ,CHangSha Branch :48%             |1   |
+---------------+------------------------------------------+---------------+----------------------------------------------------+----+

```

