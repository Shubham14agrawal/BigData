df=spark.read.csv("C:/Users/shubhamagrawal.NAGARRO/Downloads/BigData and PowerBI/InitialData.csv",header=True)
 
 
 for field in df.schema.fields:
	print(field.name +" , "+str(field.dataType))


df.show(10)

df2 = df \
  .withColumn("x",df.x.cast("int"))  \
  .withColumn("age",df.age.cast("float"))    \
  .withColumn("fnlwgt",df.fnlwgt.cast("float")) \
  .withColumn("educational-num",df["educational-num"].cast("float")) \
  .withColumn("capital-gain",df["capital-gain"].cast("float")) \
  .withColumn("capital-loss",df["capital-loss"].cast("float")) \
  .withColumn("hours-per-week",df["hours-per-week"].cast("float")) \
  

df2.select("age","educational-num").show(100)
   

df3=df2.groupBy("educational-num").count()
   
df2.summary().show()

distinctDF = df2.dropDuplicates()

dataframe.groupBy("native_country ").count().filter("`count`" <= 10).show()   
   

   


 from pyspark.sql.functions import *
df3=df2.withColumn("eligible_vote", when(col("age") >= 18, 1).otherwise(0))




df3.write.format("csv").option("header", "true").save("C:/Users/shubhamagrawal.NAGARRO/Downloads/BigData and PowerBI/proces.csv") 
