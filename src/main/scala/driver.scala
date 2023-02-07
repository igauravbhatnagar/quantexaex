package org.quantexa.exerciset1

import solutions._

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.{LocalDate, LocalDateTime}

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
  // Read Input Data (lazy)

  // Read Flights data csv
  val inputFlightsDataDF = spark.read
    .option("header", "true")
    .csv(configs.getString("paths.flightdataq1"))

  // Read Passengers data csv
  val inputPassengersDataDF = spark.read
    .option("header", "true")
    .csv(configs.getString("paths.passengersdataq2"))

  // Set Output locations
  val outputLocQ1 = configs.getString("paths.q1outlocation")
  val outputLocQ2 = configs.getString("paths.q2outlocation")
  val outputLocQ3 = configs.getString("paths.q3outlocation")
  val outputLocQ4 = configs.getString("paths.q4outlocation")
  val outputLocQ5 = configs.getString("paths.q5outlocation")

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

  val outputDF1 = solveForQ1.process(inputFlightsDataDF)

  //////////////////////////////////////////////////////
  // SOLUTION 2 - names of the 100 most frequent flyers
  //////////////////////////////////////////////////////

  val outputDF2 = solveForQ2.process(inputFlightsDataDF, 100)

  //////////////////////////////////////////////////////////////////////////////////////////////
  // SOLUTION 3 - greatest number of countries a passenger has been in without being in the UK
  //////////////////////////////////////////////////////////////////////////////////////////////

  val excludeCountry: String = "uk"
  val outputDF3 = solveForQ3.process(inputFlightsDataDF, excludeCountry)

  //////////////////////////////////////////////////////////////////////////////////////////////
  // SOLUTION 4 - Find the passengers who have been on more than 3 flights together.
  //////////////////////////////////////////////////////////////////////////////////////////////

  val atLeastNTimes = 3
  val outputDF4: DataFrame = solveForQ4.process(inputFlightsDataDF, atLeastNTimes)

  //////////////////////////////////////////////////////////////////////////////////////////////
  // SOLUTION 5 - Find the passengers who have been on more than N flights together within the range (from,to).
  //////////////////////////////////////////////////////////////////////////////////////////////

  val atLeastNTimesQ5: Int = 3
  val fromDateString: String = "2017-01-01" // format is yyyy-MM-dd
  val toDateString: String = "2017-06-01"
  val outputDF5: DataFrame = solveForBonusQ.flownTogether(inputFlightsDataDF, atLeastNTimesQ5, fromDateString, toDateString)

  // WRITE OUTPUT
  writeOutput(outputDF1, outputDF2, outputDF3, outputDF4, outputDF5)

  def writeOutput(outdfq1: DataFrame, outdfq2: DataFrame, outdfq3: DataFrame, outdfq4: DataFrame, outdfq5: DataFrame, mode: String = "both") = {
    if (mode == "fileonly" || mode == "both") {
      outdfq1.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv(outputLocQ1)
      outdfq2.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv(outputLocQ2)
      outdfq3.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv(outputLocQ3)
      outdfq4.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv(outputLocQ4)
      outdfq5.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv(outputLocQ5)
    }
    if (mode == "printonly" || mode == "both") {
      println("SOLUTION 1 - total number of flights for each month")
      outdfq1.show()
      println("SOLUTION 2 - names of the 100 most frequent flyers")
      outdfq2.show()
      println("SOLUTION 3 - greatest number of countries a passenger has been in without being in the UK")
      outdfq3.show()
      println("SOLUTION 4 - Find the passengers who have been on more than 3 flights together.")
      outdfq4.show()
      println("SOLUTION 5 - Find the passengers who have been on more than N flights together within the range (from,to)")
      outdfq5.show()
    }
  }

}