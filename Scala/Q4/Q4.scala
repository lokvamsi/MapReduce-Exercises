
var businessData = spark.read.format("csv").option("sep",":").load("FileStore/tables/business.csv")
var reviewData = spark.read.format("csv").option("delimiter",":").load("FileStore/tables/review.csv")
val newbusiness = Seq("business_id","_c1","full_address","_c3","categories");
val newreview = Seq("review_id","_c1","user_id","_c3","business_id","_c5","stars"); 
val dfBusiness = businessData.toDF(newbusiness:_*); 
val dfReview = reviewData.toDF(newreview:_*);

val tempTable1 = dfBusiness.createOrReplaceTempView("businessTable"); 
val tempTable2 = dfReview.createOrReplaceTempView("reviewTable");

val result = sqlContext.sql("SELECT rt.business_id, collect_set(bt.categories),
bt.full_address, AVG(rt.stars) FROM businessTable bt JOIN reviewTable rt ON
bt.business_id = rt.business_id GROUP BY rt.business_id, bt.full_address ORDER
BY AVG(rt.stars) DESC LIMIT 10"); 
result.show();
result.coalesce(1).rdd.saveAsTextFile("/FileStore/tables/OutputQ4.txt")
