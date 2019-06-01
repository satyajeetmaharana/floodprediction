
//Load Data set 1 : Daily Summaries Data
val df_dailySummariesData = sqlContext.load("com.databricks.spark.csv", Map("path" -> "/user/sm8235/floodprediction/NOAA_DailySummaries_Data/*"))

//Load Augmented Data set for Data set 1 for getting Station latitude and longitude
val df_stations = sqlContext.load("com.databricks.spark.csv", Map("path" -> "/user/sm8235/floodprediction/NOAA_Stations/*"))


//Load Data set 2 : NDCC Storm Data : This data set will act as our Labeled data
val df_ndccStormData = sqlContext.load("com.databricks.spark.csv", Map("path" -> "/user/sm8235/floodprediction/NDCC_StormData/*.csv"))


//Load Data set 3 : Rainfall Data : This data has the rinfall data for each day
val df_rainfallData = sqlContext.load("com.databricks.spark.csv", Map("path" -> "/user/sm8235/floodprediction/RainfallData/*.csv"))


val df_asDailySummaries = df_dailySummariesData.as("dfDailySummaries")
val df_asStations = df_stations.as("dfStations")




//Joining Daily Summaries data with Auxilary data for getting Station latitude and longitude based on Station ID
val df_dailySummaries_cordinates = df_asDailySummaries.join(df_asStations, df_asStations("ID") <=> df_asDailySummaries("STATION"), "inner")



val df_asDailySummaries_new = df_dailySummaries_cordinates.as("dfDailySummaries")
val df_asNdccStorm = df_dailySummariesData.as("dfNdccStorm")
val df_asRainfall = df_dailySummariesData.as("dfRainfall")



//Joining Daily Summaries data with the Daily Rainfall data
val joined_DailyRainfall = df_asDailySummaries_new.join(df_asRainfall, Seq("latitude","longitude"), "inner") 



val joined_asDailyRainfall = joined_DailyRainfall.as("joined_DailyRainfall")


//Joining Daily Summaries data & Rainfall data with the NDCC Storm Data which will add the labeled data to our dataset
//This would be a left join since not all days will have a record for flood or flash flood
val joined_all = joined_asDailyRainfall.join(df_asNdccStorm, Seq("latitude","longitude"),"left")


joined_all.registerTempTable("FloodData")


//Selecting only the required columns
val df_FloodData = sqlContext.sql("""SELECT date,year,month,longitude,latitude, precipitation,type,observation,event_type,flood_cause FROM FloodData""")



//Finally Saving the merged dataframe to a CSV file so we can directly use it for future models
df_FloodData.coalesce(1).write.format("com.databricks.spark.csv").save("../FloodData/Flood_Data.csv")


//Normalizing the precipitation values by using the mean value and standard deviation values
val (mean_prec, std_prec) = df_FloodData.select(mean("precipitation"), stddev("precipitation")).as[(Double, Double)].first()
df_FloodData.withColumn("precipitation_scaled", ($"precipitation" - mean_prec) / std_prec)


//Normalizing the Observation values by using the mean value and standard deviation values
val (mean_obs, std_obs) = df_FloodData.select(mean("observation"), stddev("observation")).as[(Double, Double)].first()
df_FloodData.withColumn("observation_scaled", ($"observation" - mean_obs) / std_obs)


//Labeling the data to value "1" if there was a flood on that day at those cordinates and "0" if there was no flood on that day at those cordinates
val df_FloodData_labeled = df_FloodData.map(row => {
	val row1 = row.getAs[String](8)
    val make = if (row1.toLowerCase == "flood" || row1.toLowerCase == "flash flood") "1" else "0"
    Row(row,make)
  })


//Dropping the event_type column since we already created a new column to represent the Floods data
df_FloodData_labeled.drop(df_FloodData_labeled.col("event_type"))


//Finally Saving the merged dataframe to a CSV file so we can directly use it for future models
df_FloodData_labeled.coalesce(1).write.format("com.databricks.spark.csv").save("../FloodData/Flood_Data_Merged.csv")



