`Best10_Clicking_Booking_Paying_number_By_New_Tuple_type.scala
`
## 1.functions of this system.
This package uses the project engineering method to rewrite the best10_clicking_booking_paying code.

This project uses controller/service/dao layer to process code, which has the advantage to reduce code coupling.

## 2. the util and the comm packages in this project
util: this package includes the methods to store the created SparkContext instance in the ThreadLocal  and to get the stored instance out from the ThreadLocal.

comm: this includes some methods defined in the Trait of Controller and Service and Dao.


## 3. the result :

```
category_id: 15 clicking number:6120 booking number:1672 paying number:1259
category_id: 2 clicking number:6119 booking number:1767 paying number:1196
category_id: 20 clicking number:6098 booking number:1776 paying number:1244
category_id: 12 clicking number:6095 booking number:1740 paying number:1218
category_id: 11 clicking number:6093 booking number:1781 paying number:1202
category_id: 17 clicking number:6079 booking number:1752 paying number:1231
category_id: 7 clicking number:6074 booking number:1796 paying number:1252
category_id: 9 clicking number:6045 booking number:1736 paying number:1230
category_id: 19 clicking number:6044 booking number:1722 paying number:1158
category_id: 13 clicking number:6036 booking number:1781 paying number:1161
category_id: 18 clicking number:6024 booking number:1754 paying number:1197
category_id: 5 clicking number:6011 booking number:1820 paying number:1132
category_id: 10 clicking number:5991 booking number:1757 paying number:1174
category_id: 1 clicking number:5976 booking number:1766 paying number:1191
category_id: 3 clicking number:5975 booking number:1749 paying number:1192
category_id: 8 clicking number:5974 booking number:1736 paying number:1238
category_id: 14 clicking number:5964 booking number:1773 paying number:1171
category_id: 4 clicking number:5961 booking number:1760 paying number:1271
category_id: 16 clicking number:5928 booking number:1782 paying number:1233
category_id: 6 clicking number:5912 booking number:1768 paying number:1197

```
## 3. the structure
```
E:.                                          
├─.idea                                      
│  └─codeStyles                              
├─data                                       
│  ├─adclick                                 
│  └─checkpoint                              
├─metastore_db
│  ├─log
│  ├─seg0
│  └─tmp
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
│  │      └─sparkProject
│  │         ├─Best10_Clicking_Booking_Paying
│  │         │  ├─Best10_Clicking_Booking_Paying_By_Accumulator
│  │         │  ├─Best10_Clicking_Booking_Paying_number_By_cogroup
│  │         │  ├─Best10_Clicking_Booking_Paying_number_By_New_Tuple_type
│  │         │  └─Best10_Clicking_Booking_Paying_number_By_union
│  │         ├─Best2city_ratios_in_3_category_in_eachArea
│  │         ├─BlackList_filter_create
│  │         ├─First5goodUsers_in_best10Categories_Analysis
│  │         ├─FiveMinuteFromKafkaTrend
│  │         ├─frameWork_Best10
│  │         │  ├─application
│  │         │  ├─bean
│  │         │  ├─comm
│  │         │  ├─controller
│  │         │  ├─dao
│  │         │  ├─service
│  │         │  └─util
│  │         └─PageFlows_conversion_Rate
│  └─test
│      └─java
└─target
    ├─classes
    │  └─sparkProject
    │     ├─Best10_Clicking_Booking_Paying
    │     │  ├─Best10_Clicking_Booking_Paying_By_Accumulator
    │     │  ├─Best10_Clicking_Booking_Paying_number_By_cogroup
    │     │  ├─Best10_Clicking_Booking_Paying_number_By_New_Tuple_type
    │     │  └─Best10_Clicking_Booking_Paying_number_By_union
    │     ├─Best2city_ratios_in_3_category_in_eachArea
    │     ├─BlackList_filter_create
    │     ├─First5goodUsers_in_best10Categories_Analysis
    │     ├─FiveMinuteFromKafkaTrend
    │     ├─frameWork_Best10
    │     │  ├─application
    │     │  ├─comm
    │     │  ├─controller
    │     │  ├─dao
    │     │  ├─service
    │     │  └─util
    │     └─PageFlows_conversion_Rate
    └─generated-sources
        └─annotations


```

