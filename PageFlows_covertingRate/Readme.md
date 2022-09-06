PageFlows_covertingRate.scala

## 1.functions of this system.
the system includes these functions:
1. select the first 10 categories of product in a website that have been clicked, booked and pay. 
2. list the top 5 session_id and total clicking number of each session in each 10 category.


## 2. Project resource structure


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



## 3. Code description
(First5goodUsers_in_best10Categories_Analysis.scala)
```
 /*
 1. connect to Spark
 2. parse the data to case class
 3. to compute the rate of nominator:
     A. group by session_id,
     B. sort the transaction_date
     C. form the List of pages
     D. form the consecutive pages to a List by zipping with id, and the tail of id
     E. map and reduce

 4. to compute the rate of denominator:
     A. form the List of pages
     B. map and reduce
  */
```

## 4. the result :

```
the rate of page_flow is 
the rate of from page 5 to page 39: 0.026662924501824305 and the rank: 1 
the rate of from page 48 to page 19: 0.026111111111111113 and the rank: 2 
the rate of from page 7 to page 44: 0.025952045133991537 and the rank: 3 
the rate of from page 12 to page 10: 0.025934188510875627 and the rank: 4 
the rate of from page 32 to page 26: 0.02588818507298265 and the rank: 5 
the rate of from page 21 to page 42: 0.025852272727272727 and the rank: 6 
the rate of from page 42 to page 31: 0.02577319587628866 and the rank: 7 
the rate of from page 12 to page 11: 0.025655326268823202 and the rank: 8 
the rate of from page 16 to page 18: 0.025389025389025387 and the rank: 9 
the rate of from page 26 to page 31: 0.02534059945504087 and the rank: 10 
the rate of from page 1 to page 8: 0.025274725274725275 and the rank: 11 
the rate of from page 47 to page 17: 0.025259612685938817 and the rank: 12 
the rate of from page 30 to page 34: 0.025219298245614034 and the rank: 13 
the rate of from page 10 to page 35: 0.025191675794085433 and the rank: 14 
the rate of from page 34 to page 15: 0.025136612021857924 and the rank: 15 
the rate of from page 27 to page 44: 0.025095471903982543 and the rank: 16 
the rate of from page 21 to page 30: 0.025 and the rank: 17 
the rate of from page 13 to page 15: 0.02492997198879552 and the rank: 18 
the rate of from page 37 to page 17: 0.024922118380062305 and the rank: 19 
the rate of from page 18 to page 42: 0.024503130955622107 and the rank: 20 
the rate of from page 47 to page 10: 0.024417625596407522 and the rank: 21 
the rate of from page 47 to page 37: 0.024417625596407522 and the rank: 22 
the rate of from page 41 to page 39: 0.024411257897759907 and the rank: 23 
the rate of from page 50 to page 19: 0.024349750968456003 and the rank: 24 
the rate of from page 50 to page 6: 0.024349750968456003 and the rank: 25 
the rate of from page 22 to page 17: 0.02434928631402183 and the rank: 26 
the rate of from page 45 to page 20: 0.02425729081493595 and the rank: 27 
the rate of from page 18 to page 38: 0.024230873945004085 and the rank: 28 
the rate of from page 6 to page 23: 0.024213748956303925 and the rank: 29 
the rate of from page 6 to page 33: 0.024213748956303925 and the rank: 30 
the rate of from page 48 to page 5: 0.024166666666666666 and the rank: 31 
the rate of from page 4 to page 48: 0.024153248195446973 and the rank: 32 
the rate of from page 42 to page 19: 0.024145415084102007 and the rank: 33 
the rate of from page 5 to page 2: 0.024136963233230425 and the rank: 34 
the rate of from page 47 to page 7: 0.024136963233230425 and the rank: 35 
the rate of from page 41 to page 25: 0.024124066628374498 and the rank: 36 
the rate of from page 30 to page 4: 0.02412280701754386 and the rank: 37 
the rate of from page 50 to page 50: 0.02407304925290537 and the rank: 38 
the rate of from page 50 to page 29: 0.02407304925290537 and the rank: 39 
the rate of from page 16 to page 45: 0.024024024024024024 and the rank: 40 
the rate of from page 7 to page 25: 0.023977433004231313 and the rank: 41 
the rate of from page 18 to page 24: 0.02395861693438606 and the rank: 42 
the rate of from page 39 to page 34: 0.023914238592633315 and the rank: 43 
the rate of from page 5 to page 19: 0.023856300870053325 and the rank: 44 
the rate of from page 30 to page 33: 0.023848684210526317 and the rank: 45 
the rate of from page 41 to page 9: 0.023836875358989085 and the rank: 46 
the rate of from page 11 to page 1: 0.02383654937570942 and the rank: 47 
the rate of from page 20 to page 35: 0.023835616438356164 and the rank: 48 
the rate of from page 19 to page 6: 0.023796646836127637 and the rank: 49 
the rate of from page 50 to page 1: 0.023796347537354733 and the rank: 50 
```

