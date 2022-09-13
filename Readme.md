

## 1.functions of this sparkProject.
System includes these analysis functions:
1. "Best10_Clicking_Booking_Paying"---It selects the first 10 best categories of product in a website that have been clicked, booked and pay. The sort policy is firstly to select the biggest total number of clicking. If total number of clicking were equal, then system compares the total number of booking, then total paying number.

   especially, this package includes 4 different smart and efficient methods to get the same result.

2. "BlackList_filter_create"---Using Spark Streaming, it receives the flow data from the Consumer of the Kafka. It monitors and accumulates the number of times that one user clicks on one product on a website every day. If the number of times is greater than a threshold (for example, 20 times), then the user_ID will be added to the blacklist on MySQL.

3. "Best2city_ratios_in_3_category_in_eachArea"---Using Spark SQL, it can list the most 3 popular products and related total clicking number in different area groups in China. The most complicated thing is that the spark SQL can not only list the corresponding city names in each area group, but can also use UDAF method to compute and list the 2 top transaction number ration of each city to the whole region.

4. "First5goodUsers_in_best10Categories_Analysis"---It firstly selects the first 10 categories of product in a website that have been clicked, booked and pay. and secondly it lists the top 5 session_id and total clicking number of each session in each 10 category.

5. "FiveMinuteFromKafkaTrend"--- Using Spark Streaming, it receives the data from Kafka to DStream then counts the total number of clicking within 5 minutes in each 10 seconds.

6. "PageFlows_conversion_Rate"---It counts the conversion rate of each page to the adjacent page and lists the top 50 with the highest conversion rate.





## 4. Project resource structure


```
E:.                                          
├─.idea                                      
│  └─codeStyles                              
├─data                                       
│  ├─adclick                                 
│  └─checkpoint                              
├─metastore_db
├─output
├─spark-receiver
├─src
│  ├─main
│  │  ├─java
│  │  │  └─cn
│  │  │      └─aishengying
│  │  │          ├─createorder
│  │  │          └─util
│  │  ├─resources
│  │  └─scala
│  │      ├─sparkProject
│  │      │  ├─Best10_Clicking_Booking_Paying
│  │      │  │  ├─Best10_Clicking_Booking_Paying_By_Accumulator
│  │      │  │  ├─Best10_Clicking_Booking_Paying_number_By_cogroup
│  │      │  │  ├─Best10_Clicking_Booking_Paying_number_By_New_Tuple_type
│  │      │  │  └─Best10_Clicking_Booking_Paying_number_By_union
│  │      │  ├─Best2city_ratios_in_3_category_in_eachArea
│  │      │  ├─BlackList_filter_create
│  │      │  ├─First5goodUsers_in_best10Categories_Analysis
│  │      │  ├─FiveMinuteFromKafkaTrend
│  │      │  └─PageFlows_conversion_Rate
│  │      └─util
│  └─test
│      └─java
└─target
    ├─classes
    │  ├─sparkProject
    │  │  ├─Best10_Clicking_Booking_Paying
    │  │  │  ├─Best10_Clicking_Booking_Paying_By_Accumulator
    │  │  │  ├─Best10_Clicking_Booking_Paying_number_By_cogroup
    │  │  │  ├─Best10_Clicking_Booking_Paying_number_By_New_Tuple_type
    │  │  │  └─Best10_Clicking_Booking_Paying_number_By_union
    │  │  ├─Best10_Clicking_Booking_Paying_By_Accumulator
    │  │  ├─Best2city_ratios_in_3_category_in_eachArea
    │  │  ├─BlackList_filter_create
    │  │  ├─First5goodUsers_in_best10Categories_Analysis
    │  │  ├─FiveMinuteFromKafkaTrend
    │  │  └─PageFlows_conversion_Rate
    │  └─util
    └─generated-sources
        └─annotations


```


