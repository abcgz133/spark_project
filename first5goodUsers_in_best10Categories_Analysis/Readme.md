First5goodUsers_in_best10Categories_Analysis.scala

## 1.functions of this system.
the system includes these functions:
1. select the top 10 categories of product in a website that have been clicked, booked and pay respectively. 
2. list the top 5 session_id and total clicking number of each session in each 10 category.



## 2. Code description
(First5goodUsers_in_best10Categories_Analysis.scala)
```
/*
    1. define the method(best10Category) to get the best 10 category
    2. parse the textFile to data
    3. filter the data by data whether are contained in best10Categories and is "click" transaction
    4. to map the data to a Tuple[(category_id, session_id),1L] and reduce the Tuple to ((category_id,sessionId), sum)
    5. to reform the result  from ((category_id,sessionId), sum) to (category_id,（sessionId, sum）)
    6. group by the key for (category_id,(sessionId, sum)), and sort the List(sessionId, sum) by sum
    7. print the result by take first 5 of List(sessionId, sum)


*/
```

## 3. the result :

```
First 5 users in best10 category: 
category_id:20,first 5 session_ids & total clicking number in each session_id: List((199f8e1d-db1a-4174-b0c2-ef095aaef3ee,8), (7eacf77a-c019-4072-8e09-840e5cca6569,8), (22e78a14-c5eb-45fe-a67d-2ce538814d98,7), (07b5fb82-da25-4968-9fd8-47485f4cf61e,7), (cde33446-095b-433c-927b-263ba7cd102a,7))
category_id:19,first 5 session_ids & total clicking number in each session_id: List((fde62452-7c09-4733-9655-5bd3fb705813,9), (85157915-aa25-4a8d-8ca0-9da1ee67fa70,9), (d4c2b45d-7fa1-4eff-8473-42cecdaffd62,9), (329b966c-d61b-46ad-949a-7e37142d384a,8), (1b5e5ce7-cd04-4e78-9a6f-1c3dbb29ce39,8))
category_id:15,first 5 session_ids & total clicking number in each session_id: List((632972a4-f811-4000-b920-dc12ea803a41,10), (9fa653ec-5a22-4938-83c5-21521d083cd0,8), (66a421b0-839d-49ae-a386-5fa3ed75226f,8), (5e3545a0-1521-4ad6-91fe-e792c20c46da,8), (f34878b8-1784-4d81-a4d1-0c93ce53e942,8))
category_id:2,first 5 session_ids & total clicking number in each session_id: List((66c96daa-0525-4e1b-ba55-d38a4b462b97,11), (b4589b16-fb45-4241-a576-28f77c6e4b96,11), (f34878b8-1784-4d81-a4d1-0c93ce53e942,10), (25fdfa71-c85d-4c28-a889-6df726f08ffb,9), (ab27e376-3405-46e2-82cb-e247bf2a16fb,8))
category_id:17,first 5 session_ids & total clicking number in each session_id: List((4509c42c-3aa3-4d28-84c6-5ed27bbf2444,12), (bf390289-5c9d-4037-88b3-fdf386b3acd5,8), (1b5ac69b-5e00-4ff3-8a5c-6822e92ecc0c,8), (0416a1f7-350f-4ea9-9603-a05f8cfa0838,8), (9bdc044f-8593-49fc-bbf0-14c28f901d42,8))
category_id:13,first 5 session_ids & total clicking number in each session_id: List((329b966c-d61b-46ad-949a-7e37142d384a,8), (f736ee4a-cc14-4aa9-9a96-a98b0ad7cc3d,8), (1fb79ba2-4780-4652-9574-b1577c7112db,7), (0f227059-7006-419c-87b0-b2057b94505b,7), (632972a4-f811-4000-b920-dc12ea803a41,7))
category_id:11,first 5 session_ids & total clicking number in each session_id: List((329b966c-d61b-46ad-949a-7e37142d384a,12), (99f48b83-8f85-4bea-8506-c78cfe5a2136,7), (4509c42c-3aa3-4d28-84c6-5ed27bbf2444,7), (dc226249-ce13-442c-b6e4-bfc84649fff6,7), (2cd89b09-bae3-49b5-a422-9f9e0c12a040,7))
category_id:7,first 5 session_ids & total clicking number in each session_id: List((a41bc6ea-b3e3-47ce-98af-48169da7c91b,9), (4dbd319c-3f44-48c9-9a71-a917f1d922c1,7), (9fa653ec-5a22-4938-83c5-21521d083cd0,7), (aef6615d-4c71-4d39-8062-9d5d778e55f1,7), (2d4b9c3e-2a9e-41b6-9573-9fde3533ed89,7))
category_id:9,first 5 session_ids & total clicking number in each session_id: List((199f8e1d-db1a-4174-b0c2-ef095aaef3ee,9), (329b966c-d61b-46ad-949a-7e37142d384a,8), (5e3545a0-1521-4ad6-91fe-e792c20c46da,8), (66c96daa-0525-4e1b-ba55-d38a4b462b97,7), (e306c00b-a6c5-44c2-9c77-15e919340324,7))
category_id:12,first 5 session_ids & total clicking number in each session_id: List((a4b05ea2-2869-4f20-a82a-86352aa60e9f,8), (b4589b16-fb45-4241-a576-28f77c6e4b96,8), (22a687a0-07c9-4e84-adff-49dfc4fe96df,8), (73203aee-de2e-443e-93cb-014e38c0d30c,8), (64285623-54ad-4a1f-ae84-d8f85ebf94c6,7))

```

