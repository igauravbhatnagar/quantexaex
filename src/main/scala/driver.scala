package org.quantexa.exerciset1

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, date_format, desc, to_date}

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

object driver extends App {

  // /////////////////////////////////////////////
  // Spark Session
  ///////////////////////////////////////////////
  val spark = SparkSession.builder().config("spark.master", "local")
    .appName("quantexaex")
    .getOrCreate()

  ///////////////////////////////////////////////
  // Spark configs and initial variables
  ///////////////////////////////////////////////
  val configs: Config = ConfigFactory.load("properties.conf")

  // Set Logger
  val rootLogger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)

  ///////////////////////////////////////////////
  //SOLUTION
  ///////////////////////////////////////////////
  rootLogger.info("---Solution 1---")
  println("---Solution 1---")

  // Read Input Data (lazy)

  // Read Flights data csv
  val inputFlightsData = spark.read
                          .option("header", "true")
                          .csv(configs.getString("paths.flightdataq1"))

  // Read Passengers data csv
  val inputPassengersData = spark.read
                          .option("header", "true")
                          .csv(configs.getString("paths.passengersdataq2"))

  // Set Output locations
  val outputLocQ1 = configs.getString("paths.q1outlocation")
  val outputLocQ2 = configs.getString("paths.q2outlocation")
  val outputLocQ3 = configs.getString("paths.q3outlocation")
  val outputLocQ4 = configs.getString("paths.q4outlocation")

  // Date and Time Variables

  val dateToday = LocalDate.now()
  val TimeToday = LocalDateTime.now()
  val dateFormat = "ddMMYYYY"
  val fmtYear = "YYYY"
  val fmtMonth = "MM"
  val fmtDay = "dd"
  val TimeStampFormat = "ddMMYYYY"

  //////////////////////////////////////////////////////
  // SOLUTION 1 - total number of flights for each month
  //////////////////////////////////////////////////////
  val outputDF1 = inputFlightsData.withColumn("Month",date_format(col("date"),fmtMonth))
                  .groupBy("Month").count.withColumnRenamed("count","Number_of_FLights")
                  .orderBy("Month").withColumn("Month",col("Month").cast("Int"))

  //////////////////////////////////////////////////////
  // SOLUTION 2 - names of the 100 most frequent flyers
  //////////////////////////////////////////////////////

  val frequentFlierDF = inputFlightsData.groupBy("passengerId").count().withColumnRenamed("count","Number_of_FLights")
  val outputDF2 = frequentFlierDF.join(inputPassengersData,"passengerId").select("passengerId","Number_of_flights","firstName","lastName")
    .orderBy(desc("Number_of_flights")).limit(100)

  //////////////////////////////////////////////////////////////////////////////////////////////
  // SOLUTION 2 - greatest number of countries a passenger has been in without being in the UK
  //////////////////////////////////////////////////////////////////////////////////////////////



  writeOutput(outputDF1,outputDF2)
  // WRITE OUTPUT
  def writeOutput(outdfq1:DataFrame,outdfq2:DataFrame) = {
    outdfq1.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv(outputLocQ1)
    outdfq2.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv(outputLocQ2)
  }

}