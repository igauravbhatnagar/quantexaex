package org.quantexa.exerciset1

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object driver extends App {

  val spark = SparkSession.builder().config("spark.master", "local")
    .appName("quantexaex")
    .getOrCreate()
  //  val sc = spark.sparkContext

  //-----------------------------------------------
  // Spark configs and initial variables
  //-----------------------------------------------

  val configs: Config = ConfigFactory.load("properties.conf")

  // Logger
  val rootLogger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)

  //*************************************
  //SOLUTION                            *
  //*************************************
  rootLogger.info("---Solution 1---")
  println("---Solution 1---")

  //Input data

  // Read Flights data csv
  val inputFlightsData = spark.read
                          .option("header", "true")
                          .csv(configs.getString("paths.flightdataq1"))
  // Read Passengers data csv
  val inputPassengersData = spark.read
                          .option("header", "true")
                          .csv(configs.getString("paths.passengersdataq2"))


}