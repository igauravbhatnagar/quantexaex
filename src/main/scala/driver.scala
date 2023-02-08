package org.quantexa.exerciset1

import solutions._
import utils.userDefinedFunctions._

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

  // Read Input Data

  // Read Flights data csv
  val inputFlightsDataDF = spark.read
    .option("header", "true")
    .csv(configs.getString("paths.flightdataq1"))

  // Read Passengers data csv
  val inputPassengersDataDF = spark.read
    .option("header", "true")
    .csv(configs.getString("paths.passengersdataq2"))

  // Date and Time Variables

  val dateToday = LocalDate.now()
  val TimeToday = LocalDateTime.now()

  val fmtMonth = "MM"

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

  val excludeCountry: String = configs.getString("jobParams.excludeCountryForQ3")
  val outputDF3 = solveForQ3.process(inputFlightsDataDF, excludeCountry)

  //////////////////////////////////////////////////////////////////////////////////////////////
  // SOLUTION 4 - Find the passengers who have been on more than 3 flights together.
  //////////////////////////////////////////////////////////////////////////////////////////////

  val atLeastNTimes = configs.getInt("jobParams.atLeastNTimesForQ4")
  val outputDF4: DataFrame = solveForQ4.process(inputFlightsDataDF, atLeastNTimes)

  //////////////////////////////////////////////////////////////////////////////////////////////
  // SOLUTION 5 - Find the passengers who have been on more than N flights together within the range (from,to).
  //////////////////////////////////////////////////////////////////////////////////////////////

  val atLeastNTimesQ5: Int = configs.getInt("jobParams.atLeastNTimesForQ4")
  val fromDateString: String = configs.getString("jobParams.fromDateStringForQ5")  // format is yyyy-MM-dd
  val toDateString: String = configs.getString("jobParams.toDateStringForQ5")
  val outputDF5: DataFrame = solveForBonusQ.flownTogether(inputFlightsDataDF, atLeastNTimesQ5, fromDateString, toDateString)

  // WRITE OUTPUT
  generateOutput(outputDF1, outputDF2, outputDF3, outputDF4, outputDF5, configs.getString("jobParams.outputMode"))

}