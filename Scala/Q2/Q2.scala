val file = sc.textFile("FileStore/tables/friends.txt")   
val pairs = file.map(line=>line.split("\\t"))
  .filter(line => line.size == 2)
  .map(line =>(line(0),line(1).split(",").toList))

var pairsFlat = pairs.flatMap(pair => pair._2.map(pairList =>
                              if(pair._1.toInt > pairList.toInt){
                                ((pairList,pair._1),pair._2)
                              }else{
                                ((pair._1,pairList),pair._2)
                              }
                              ))
pairsFlat.collect()
val mutualFriendsList = pairsFlat.reduceByKey((x,y) => x.intersect(y))

val mutualFriendListSize = mutualFriendsList.map(pair => (pair._1,pair._2.size))

val sortData = mutualFriendListSize.sortBy(_._2, false).take(10)

val userfile = sc.textFile("FileStore/tables/userdata.txt")
val userSplit = userfile.map(line => line.split(","))

val userData = userSplit.map(line => (line(0), List(line(1),line(2),line(3))))

val result = sc.parallelize(sortData.map(line=>(line._2+"\t"+userData.lookup(line._1._1)(0).mkString("\t")+"\t"+userData.lookup(line._1._2)(0).mkString("\t"))));

result.coalesce(1).saveAsTextFile("/FileStore/Tables/OutputQ2.txt")
