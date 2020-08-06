var businessData = spark.read.format("csv").option("sep",":").load("FileStore/tables/business.csv")
var reviewData = spark.read.format("csv").option("delimiter",":").load("FileStore/tables/review.csv")
val newbusiness = Seq("business_id","_c1","full_address","_c3","categories");
val newreview = Seq("review_id","_c1","user_id","_c3","business_id","_c5","stars"); 
val dfBusiness = businessData.toDF(newbusiness:_*); 
val dfReview = reviewData.toDF(newreview:_*);

val x = dfBusiness.select("business_id" ,"full_address"); 
val y = dfBusiness.select("full_address"); 
var userid = dfReview.select("user_id");
var rate = dfReview.select("stars"); 
val joined = x.join(dfReview , "business_id");

val result = joined.filter($"full_address".contains("Stanford")).select("user_id","stars").distinct
result.show();
result.coalesce(1).redd.saveAsTextFile("/FileStore/tables/OutputQ3.txt")