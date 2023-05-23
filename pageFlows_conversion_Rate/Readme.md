PageFlows_conversion_Rate.scala

## 1.functions of this system.
The system counts the conversion rate of each page to the adjacent page and lists the top 50 with the highest conversion rate.
## 2. Code description
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

## 3. the result :

```
the rate of page_flow is 
the rate of from page 5 to page 39: 0.02666 and the rank: 1 
the rate of from page 48 to page 19: 0.02611 and the rank: 2 
the rate of from page 7 to page 44: 0.02595 and the rank: 3 
the rate of from page 12 to page 10: 0.02593 and the rank: 4 
the rate of from page 32 to page 26: 0.02589 and the rank: 5 
the rate of from page 21 to page 42: 0.02585 and the rank: 6 
the rate of from page 42 to page 31: 0.02577 and the rank: 7 
the rate of from page 12 to page 11: 0.02566 and the rank: 8 
the rate of from page 16 to page 18: 0.02539 and the rank: 9 
the rate of from page 26 to page 31: 0.02534 and the rank: 10 
the rate of from page 1 to page 8: 0.02527 and the rank: 11 
the rate of from page 47 to page 17: 0.02526 and the rank: 12 
the rate of from page 30 to page 34: 0.02522 and the rank: 13 
the rate of from page 10 to page 35: 0.02519 and the rank: 14 
the rate of from page 34 to page 15: 0.02514 and the rank: 15 
the rate of from page 27 to page 44: 0.02510 and the rank: 16 
the rate of from page 21 to page 30: 0.02500 and the rank: 17 
the rate of from page 13 to page 15: 0.02493 and the rank: 18 
the rate of from page 37 to page 17: 0.02492 and the rank: 19 
the rate of from page 18 to page 42: 0.02450 and the rank: 20 
the rate of from page 47 to page 10: 0.02442 and the rank: 21 
the rate of from page 47 to page 37: 0.02442 and the rank: 22 
the rate of from page 41 to page 39: 0.02441 and the rank: 23 
the rate of from page 50 to page 19: 0.02435 and the rank: 24 
the rate of from page 50 to page 6: 0.02435 and the rank: 25 
the rate of from page 22 to page 17: 0.02435 and the rank: 26 
the rate of from page 45 to page 20: 0.02426 and the rank: 27 
the rate of from page 18 to page 38: 0.02423 and the rank: 28 
the rate of from page 6 to page 23: 0.02421 and the rank: 29 
the rate of from page 6 to page 33: 0.02421 and the rank: 30 
the rate of from page 48 to page 5: 0.02417 and the rank: 31 
the rate of from page 4 to page 48: 0.02415 and the rank: 32 
the rate of from page 42 to page 19: 0.02415 and the rank: 33 
the rate of from page 5 to page 2: 0.02414 and the rank: 34 
the rate of from page 47 to page 7: 0.02414 and the rank: 35 
the rate of from page 41 to page 25: 0.02412 and the rank: 36 
the rate of from page 30 to page 4: 0.02412 and the rank: 37 
the rate of from page 50 to page 50: 0.02407 and the rank: 38 
the rate of from page 50 to page 29: 0.02407 and the rank: 39 
the rate of from page 16 to page 45: 0.02402 and the rank: 40 
the rate of from page 7 to page 25: 0.02398 and the rank: 41 
the rate of from page 18 to page 24: 0.02396 and the rank: 42 
the rate of from page 39 to page 34: 0.02391 and the rank: 43 
the rate of from page 5 to page 19: 0.02386 and the rank: 44 
the rate of from page 30 to page 33: 0.02385 and the rank: 45 
the rate of from page 41 to page 9: 0.02384 and the rank: 46 
the rate of from page 11 to page 1: 0.02384 and the rank: 47 
the rate of from page 20 to page 35: 0.02384 and the rank: 48 
the rate of from page 19 to page 6: 0.02380 and the rank: 49 
the rate of from page 50 to page 1: 0.02380 and the rank: 50 
```

