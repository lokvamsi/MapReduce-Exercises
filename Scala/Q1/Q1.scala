val file = sc.textFile("FileStore/tables/friends.txt")   
val pairs = file.map(line=>line.split("\\t"))
  .filter(line => (line.size == 2))
  .map(line=>(line(0),line(1).split(",").toList))
var temp1 = pairs.flatMap(x=>x._2.map(z=>
                           if(x._1.toInt < z.toInt)
                                        {((x._1,z),x._2)}
                           else
                                          {((z,x._1),x._2)}
                          ))  
val mutualFriends = temp1.reduceByKey((x,y) => x.intersect(y))
val result = mutualFriends.map(li => li._1._1+"\t"+li._1._2+"\t"+li._2.mkString(","));
result.saveAsTextFile("/FileStore/tables/OutputQ1.txt")
