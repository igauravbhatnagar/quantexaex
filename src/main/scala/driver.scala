package org.quantexa.exerciset1

import com.typesafe.config.{Config, ConfigFactory}
import javafx.beans.binding.When
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, date_format, desc, lag, lit, to_date, when}
import org.apache.spark.sql.functions._
import org.quantexa.exerciset1.solutions._

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

  val outputDF4: DataFrame = solveForQ4.process(inputFlightsDataDF)


  // WRITE OUTPUT
  writeOutput(outputDF1,outputDF2,outputDF3,outputDF4)
  def writeOutput(outdfq1:DataFrame,outdfq2:DataFrame,outdfq3:DataFrame,outdfq4:DataFrame) = {
    outdfq1.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv(outputLocQ1)
    outdfq2.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv(outputLocQ2)
    outdfq3.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv(outputLocQ3)
    outdfq4.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv(outputLocQ4)
  }

}