![avatar](./images/Trend_payment.png)
## 1.Functions of this credit card sparkProject
System includes these analysis functions:
1. "Trend_Payment_type_From_Kafka"--- By using Spark Streaming, it analyzes the transaction payment trend dynamically in each 10 seconds by Spark Streaming. It can receive the Kafka flowing data in each 10 seconds and accumulate the transaction number. Secondly, it can show the trend of 3 different payment types(01--Alipay, 02--WeChat pay, 03--other payments) and the total number of payment transaction in each day in a BI tool(FineBI) graphically  and intuitively.
2. "Transactions_Real_Time_Risk_Monitoring_and_processing"---A transaction risk monitor and control system. System can receive the flow data from the Consumer of Kafka. It then monitors and accumulates the number of times that one credit card being used in one merchant in each day. If the number of times is greater than a threshold (for example, 20 times), then the card will be added to the blacklist on MySQL.
3. "Card_type_Best_market_share_ratio_in_Branches"---Listing the top 3 total transaction number of credit card types and ranking in each Chinese areas, importantly, also listing the market share proportion and ranking of these card types in different branches.
4. "Others"---including other market business analysis. such as counting the user page-clicking conversion rate in internet(counting each page to the adjacent page) and listing the top 50 with the highest conversion rate, etc.


## 2. Result of these functions(partial):
(1) Trend_Payment_type_From_Kafka
![avatar](./images/Trend_payment.png)


(2) card_type_Best_market_share_ratio_in_Branches 
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



