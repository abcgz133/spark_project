`Best10_Clicking_Booking_Paying_number_By_New_Tuple_type.scala
`
## 1.functions of this system.
the system includes these functions:
1. It selects the first 10 best categories of product in a website that have been clicked, booked and pay.
2. to select these best categories efficiently, it mapped a tuple for each line in the data.
   If the data is a clicking action, then the tuple is (1,0,0),
   if the data is a booking action, then tuple is (0,1,0),
   if the data is a paying action, then tuple is (0,0,1)
3. reduce this mapped data by adding corresponding element by key.
4. The sort policy is firstly to select the biggest total number of clicking. If total number of clicking were equal, then system compares the total number of booking, then total paying number.

## 2. benefit of this new Tuple structure
This new tuple can avoid too many shuffles. The program firstly converts the RDD to (categoryID, (1, 0, 0)) instead of the reduceByKey function at first.


## 3. Code description
(Best10Clicking_Booking_Paying.scala)
```
/*
    1. connect to Spark
    2. filter and map the data to New_tuple_type
        if clicking: (category_id, 1, 0, 0)
        if booking: (category_id, 0, 1, 0)
        if paying: (category_id, 0, 0, 1)
    3. reduce the new_tuple_type
    4. sort the tuple by (clicking_number, booking_number, paying_number)
    5. print on the console
*/
```

## 4. the result :

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

