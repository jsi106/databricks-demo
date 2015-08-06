// Databricks notebook source exported at Thu, 6 Aug 2015 18:50:34 UTC
val a = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee","tiger"), 3)
val b = sc.parallelize(List(1,1,2,2,2,1,2,2,2), 3)
val c = b.zip(a)
val d = c.combineByKey(List(_), (x:List[String], y:String) => y :: x, (x:List[String], y:List[String]) => x ::: y)

// COMMAND ----------

c.collect

// COMMAND ----------


val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
val b = a.keyBy(_.length)
val c = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
val d = c.keyBy(_.length)

// COMMAND ----------

b.collect

// COMMAND ----------

d.collect

// COMMAND ----------

b.join(d).collect

// COMMAND ----------

range(1,10)

// COMMAND ----------

val wordsList = List("cat", "elephant", "rat", "rat", "cat")
val wordsRDD = sc.parallelize(wordsList, 4)

val sRDDMap = wordsRDD.map(x => (x, x + "s"))
println(sRDDMap.collect())
println(sRDDMap.count())
val sRDD = wordsRDD.flatMap(x => List(x, x + "s"))
println(sRDD.collect())
println(sRDD.count())

// COMMAND ----------

val z = sc.parallelize(List(1,2,3,4,5,6), 3)
z.aggregate(0)(math.max(_, _), _ + _)


// COMMAND ----------

