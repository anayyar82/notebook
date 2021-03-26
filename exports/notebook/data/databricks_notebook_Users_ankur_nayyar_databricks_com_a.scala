// Databricks notebook source
def showProp(prop: String) = println(s"$prop=${spark.conf.get(prop,null)}")
showProp("spark.databricks.delta.preview.enabled")

afdsafdsf
fasddsf


// COMMAND ----------

// MAGIC %r 
// MAGIC 
// MAGIC library(dplyr)

// COMMAND ----------

// MAGIC %r
// MAGIC 
// MAGIC dplyr::filter(mtcars, mpg == 21)

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC from pyspark.sql.functions import pandas_udf, PandasUDFType
// MAGIC 
// MAGIC @pandas_udf("vector", PandasUDFType.SCALAR)
// MAGIC def get_vector_class(vec, classes):
// MAGIC   return vec #classes[np.argmax(vec, axis=1)]

// COMMAND ----------

val profiler = org.apache.spark.SparkEnv.get.profiler


// COMMAND ----------

profiler.warmUp() // This can take a while


// COMMAND ----------

val run = profiler.run()
 
def df = spark.range(100000).groupBy().avg()
df.show()
 
val profiles = run.awaitResults()

// COMMAND ----------

val run = run.awaitResults(60 * 1000L)


// COMMAND ----------

val links = profiles.map { profile =>
  val name = profile.stripPrefix("dbfs:/FileStore/profiles/")
  s"""<a href="files/profiles/$name" download>Download $name</a>"""
}
displayHTML(links.mkString("<br/>"))

// COMMAND ----------

// MAGIC %r
// MAGIC install.packages("keras", dependencies = TRUE)

// COMMAND ----------

// MAGIC %sh
// MAGIC 
// MAGIC #!/bin/bash 
// MAGIC sudo apt-get update
// MAGIC sudo apt-get install -y maven
// MAGIC sudo apt-get -y install git
// MAGIC sudo git clone --recursive https://github.com/dmlc/xgboost

// COMMAND ----------

// MAGIC %fs ls

// COMMAND ----------

// MAGIC %sh rm -r xgboost

// COMMAND ----------

// MAGIC %sh
// MAGIC 
// MAGIC #!/bin/bash 
// MAGIC sudo apt-get update
// MAGIC sudo apt-get install -y maven
// MAGIC sudo apt-get -y install git
// MAGIC sudo git clone --recursive https://github.com/dmlc/xgboost
// MAGIC cd xgboost
// MAGIC sudo git checkout tags/v0.7 -b xbgoost_v0.7
// MAGIC 
// MAGIC export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
// MAGIC 
// MAGIC find * -name tracker.py -type f -print0 | xargs -0 sed -i '' "s/port=9091/port=33000/g"
// MAGIC make -j4
// MAGIC 
// MAGIC cd jvm-packages
// MAGIC 
// MAGIC sudo apt-get -y install maven
// MAGIC sudo apt-get -y install cmake
// MAGIC mvn -DskipTests=true package
// MAGIC mkdir -p /databricks/jars/
// MAGIC cp xgboost4j-spark/target/xgboost4j-spark-0.7-jar-with-dependencies.jar /databricks/jars/xgboost4j-spark-0.7-jar-with-dependencies.jar

// COMMAND ----------

// MAGIC %sql set spark.speculation = false

// COMMAND ----------


dbutils.fs.put("/databricks/init/xgboost/install-xgboost.sh", """
#!/bin/bash 
sudo apt-get update
sudo apt-get install -y maven
sudo apt-get -y install git
sudo git clone --recursive https://github.com/tomasatdatabricks/xgboost-spark-linux64

cd xgboost-spark-linux64
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/

find * -name tracker.py -type f -print0 | xargs -0 sed -i '' "s/port=9091/port=33000/g"
make -j4

cd jvm-packages

sudo apt-get -y install maven
sudo apt-get -y install cmake
mvn -DskipTests=true package
mkdir -p /databricks/jars/
cp xgboost4j-spark/target/xgboost4j 0.8-SNAPSHOT.jar /databricks/jars/xgboost4j 0.8-SNAPSHOT.jar
""", true)

// COMMAND ----------

// MAGIC %sh 
// MAGIC #!/bin/bash 
// MAGIC sudo apt-get update
// MAGIC sudo apt-get install -y maven
// MAGIC sudo apt-get -y install git
// MAGIC sudo git clone --recursive https://github.com/dmlc/xgboost/releases/tag/v0.7
// MAGIC 
// MAGIC cd xgboost
// MAGIC export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
// MAGIC 
// MAGIC find * -name tracker.py -type f -print0 | xargs -0 sed -i '' "s/port=9091/port=33000/g"
// MAGIC make -j4
// MAGIC 
// MAGIC cd jvm-packages
// MAGIC 
// MAGIC #Change Scala Version
// MAGIC #sed -i -r -e 's/scala.version.([0-9.]+)/scala.version\>2.10.6/g' /xgboost/jvm-packages/pom.xml
// MAGIC 
// MAGIC sudo apt-get -y install maven
// MAGIC sudo apt-get -y install cmake
// MAGIC mvn -DskipTests=true package
// MAGIC mkdir -p /databricks/jars/
// MAGIC cp xgboost4j-spark/target/xgboost4j-spark-0.7-jar-with-dependencies.jar /databricks/jars/xgboost4j-spark-0.7-with-dependencies.jar

// COMMAND ----------

dbutils.fs.put("/databricks/init/rj_xgboost_1/install-xgboost.sh", """
#!/bin/bash 
sudo apt-get update
sudo apt-get install -y maven
sudo apt-get -y install git
sudo git clone --recursive https://github.com/dmlc/xgboost/releases/tag/v0.7

cd xgboost
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/

find * -name tracker.py -type f -print0 | xargs -0 sed -i '' "s/port=9091/port=33000/g"
make -j4

cd jvm-packages

#Change Scala Version
#sed -i -r -e 's/scala.version.([0-9.]+)/scala.version\>2.10.6/g' /xgboost/jvm-packages/pom.xml

sudo apt-get -y install maven
sudo apt-get -y install cmake
mvn -DskipTests=true package
mkdir -p /databricks/jars/
cp xgboost4j-spark/target/xgboost4j-example 0.8-SNAPSHOT.jar /databricks/jars/xgboost4j-example 0.8-SNAPSHOT.jar
""", true)

// COMMAND ----------

// MAGIC %sh
// MAGIC 
// MAGIC #!/bin/bash 
// MAGIC sudo apt-get update
// MAGIC sudo apt-get install -y maven
// MAGIC sudo apt-get -y install git
// MAGIC sudo git clone --recursive https://github.com/tomasatdatabricks/xgboost
// MAGIC 
// MAGIC cd xgboost
// MAGIC export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
// MAGIC 
// MAGIC find * -name tracker.py -type f -print0 | xargs -0 sed -i '' "s/port=9091/port=33000/g"
// MAGIC make -j4
// MAGIC 
// MAGIC cd jvm-packages
// MAGIC 
// MAGIC sudo apt-get -y install maven
// MAGIC sudo apt-get -y install cmake
// MAGIC mvn -DskipTests=true package
// MAGIC mkdir -p /databricks/jars/
// MAGIC cp xgboost4j-spark/target/xgboost4j-spark-0.8-SNAPSHOT.jar /databricks/jars/xgboost4j-spark-0.8-SNAPSHOT.jar

// COMMAND ----------

// MAGIC %sh 
// MAGIC 
// MAGIC ls /databricks/driver

// COMMAND ----------

dbutils.fs.put("/databricks/init/xgboost_ds_1/install-xgboost.sh", """
#!/bin/bash 
sudo apt-get update
sudo apt-get install -y maven
sudo apt-get -y install git
sudo git clone --recursive https://github.com/tomasatdatabricks/xgboost

cd xgboost
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/

find * -name tracker.py -type f -print0 | xargs -0 sed -i '' "s/port=9091/port=33000/g"
make -j4

cd jvm-packages

sudo apt-get -y install maven
sudo apt-get -y install cmake
mvn -DskipTests=true package
mkdir -p /databricks/jars/
cp xgboost4j-spark/target/xgboost4j-spark-0.8-SNAPSHOT.jar /databricks/jars/xgboost4j-spark-0.8-SNAPSHOT.jar
""", true)

// COMMAND ----------

// MAGIC %sh 
// MAGIC #!/bin/bash 
// MAGIC sudo apt-get update
// MAGIC sudo apt-get install -y maven
// MAGIC sudo apt-get -y install git
// MAGIC sudo git clone --recursive https://github.com/dmlc/xgboost/tree/v0.7
// MAGIC 
// MAGIC cd xgboost
// MAGIC export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
// MAGIC 
// MAGIC find * -name tracker.py -type f -print0 | xargs -0 sed -i '' "s/port=9091/port=33000/g"
// MAGIC make -j4
// MAGIC 
// MAGIC cd jvm-packages
// MAGIC 
// MAGIC #Change Scala Version
// MAGIC #sed -i -r -e 's/scala.version.([0-9.]+)/scala.version>2.10.6/g' /xgboost/jvm-packages/pom.xml
// MAGIC 
// MAGIC sudo apt-get -y install maven
// MAGIC sudo apt-get -y install cmake
// MAGIC mvn -DskipTests=true package
// MAGIC mkdir -p /databricks/jars/
// MAGIC cp xgboost4j-spark/target/xgboost4j-spark-0.8-SNAPSHOT-jar-with-dependencies.jar /databricks/jars/xgboost4j-spark-0.8-SNAPSHOT-jar-with-dependencies.jar

// COMMAND ----------

dbutils.fs.put("/databricks/init/rj_xgboost_spark_2.1/install-xgboost.sh", """
#!/bin/bash 
sudo apt-get update
sudo apt-get install -y maven
sudo apt-get -y install git
sudo git clone --recursive https://github.com/dmlc/xgboost/tree/v0.7

cd xgboost
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/

find * -name tracker.py -type f -print0 | xargs -0 sed -i '' "s/port=9091/port=33000/g"
make -j4

cd jvm-packages

#Change Scala Version
#sed -i -r -e 's/scala.version.([0-9.]+)/scala.version>2.10.6/g' /xgboost/jvm-packages/pom.xml

sudo apt-get -y install maven
sudo apt-get -y install cmake
mvn -DskipTests=true package
mkdir -p /databricks/jars/
cp xgboost4j-spark/target/xgboost4j-spark-0.8-SNAPSHOT-jar-with-dependencies.jar /databricks/jars/xgboost4j-spark-0.8-SNAPSHOT-jar-with-dependencies.jar
""", true)

// COMMAND ----------

dbutils.fs.put("/databricks/init/rj_xgboost_1/install-xgboost.sh", """
#!/bin/bash 
sudo apt-get update
sudo apt-get install -y maven
sudo apt-get -y install git
sudo git clone --recursive https://github.com/dmlc/xgboost

cd xgboost
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/

find * -name tracker.py -type f -print0 | xargs -0 sed -i '' "s/port=9091/port=33000/g"
make -j4

cd jvm-packages

#Change Scala Version
#sed -i -r -e 's/scala.version.([0-9.]+)/scala.version\>2.10.6/g' /xgboost/jvm-packages/pom.xml

sudo apt-get -y install maven
sudo apt-get -y install cmake
mvn -DskipTests=true package
mkdir -p /databricks/jars/
cp xgboost4j-spark/target/xgboost4j-spark-0.8-SNAPSHOT-jar-with-dependencies.jar /databricks/jars/xgboost4j-spark-0.8-SNAPSHOT-jar-with-dependencies.jar
""", true)

// COMMAND ----------

// MAGIC %fs ls

// COMMAND ----------

// MAGIC %sh 
// MAGIC 
// MAGIC #!/bin/bash 
// MAGIC sudo apt-get update
// MAGIC sudo apt-get install -y maven
// MAGIC sudo apt-get -y install git
// MAGIC sudo git clone --recursive https://github.com/dmlc/xgboost
// MAGIC 
// MAGIC cd xgboost
// MAGIC export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
// MAGIC 
// MAGIC find * -name tracker.py -type f -print0 | xargs -0 sed -i '' "s/port=9091/port=33000/g"
// MAGIC make -j4
// MAGIC 
// MAGIC cd jvm-packages
// MAGIC 
// MAGIC #Change Scala Version
// MAGIC #sed -i -r -e 's/scala.version.([0-9.]+)/scala.version\>2.10.6/g' /xgboost/jvm-packages/pom.xml
// MAGIC 
// MAGIC sudo apt-get -y install maven
// MAGIC sudo apt-get -y install cmake
// MAGIC mvn -DskipTests=true package
// MAGIC mkdir -p /databricks/jars/
// MAGIC cp xgboost4j-spark/target/xgboost4j-spark-0.8-SNAPSHOT-jar-with-dependencies.jar /databricks/jars/xgboost4j-spark-0.8-SNAPSHOT-jar-with-dependencies.jar

// COMMAND ----------

// MAGIC %sh cd xgboost

// COMMAND ----------

// MAGIC %ls spark.driver.maxResultSize 500g