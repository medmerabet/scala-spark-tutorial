package com.sparkTutorial.sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions.array_contains


object StackOverFlowSurvey {

  val AGE_MIDPOINT = "age_midpoint"
  val SALARY_MIDPOINT = "salary_midpoint"
  val SALARY_MIDPOINT_BUCKET = "salary_midpoint_bucket"

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("StackOverFlowSurvey").master(local[*]).getOrCreate()
    val dataFrameReader = session.read

    val responses = dataFrameReader
      .option("header", "true")
      .option("inferSchema", value = true)
      .csv("in/2016-stack-overflow-survey-responses.csv")

    System.out.println("=== Le shéma de la donnée source ===")
    responses.printSchema()


    val responseWithSelectedColumns = responses.select("country", "occupation", AGE_MIDPOINT, SALARY_MIDPOINT)

    System.out.println("=== On affiche les columns séléctionnées ===")
    responseWithSelectedColumns.show()

    System.out.println("=== Quand le pays = Afghanistan ===")
    responseWithSelectedColumns.filter(responseWithSelectedColumns.col("country").===("Algeria")).show()

    System.out.println("=== On fait un count ===")
    val groupedDataset = responseWithSelectedColumns.groupBy("occupation")
    groupedDataset.count().show()

    System.out.println("=== Quand la moyenne d'age < 20 ===")
    responseWithSelectedColumns.filter(responseWithSelectedColumns.col(AGE_MIDPOINT) < 20).show()

    System.out.println("=== Les salaire moyen décroissant ===")
    responseWithSelectedColumns.orderBy(responseWithSelectedColumns.col(SALARY_MIDPOINT).desc).show()

    System.out.println("=== Group by country  ===")
    val datasetGroupByCountry = responseWithSelectedColumns.groupBy("country")
    datasetGroupByCountry.avg(SALARY_MIDPOINT).show()

    val responseWithSalaryBucket = responses.withColumn(SALARY_MIDPOINT_BUCKET,
      responses.col(SALARY_MIDPOINT).divide(20000).cast("integer").multiply(20000))

    System.out.println("=== Group by salary bucket ===")
    responseWithSalaryBucket.groupBy(SALARY_MIDPOINT_BUCKET).count().orderBy(SALARY_MIDPOINT_BUCKET).show()

    session.stop()
  }
}
