# Flood Prediction in the United States using machine learning methods

## Introduction

  Floods can be categorized into both slow- developing flowing water and flash flowing water. Floods are temporary but have an exceptional  impact on the area while flooding. We built a Random Forest prediction model based on and visualize the probability of flooding among different areas in Tableau. We also built a small email service to alert counties about the possibility of future flooding. We hope that our application can help with future flood detection and prevent within the United States.
  
  We collected the datasets from three different sources provided by the United States National Climatic Data Center (NCDC) and the National Oceanic and Atmospheric Administration (NOAA). Then, we analyzed and extracted important features such as the amount of precipitation and wind in certain areas out of the datasets. We built the random forest prediction model using MLlib in Apache Spark to predict the outcomes of future floods. We then built a Tableau workbook to visualize the results of flood prediction and a small application for emailing county officials about upcoming floods in their area.

We created a flood prediction application that uses various sources of publicly available weather data with a random forest model. After cleaning and extracting features from the data, we utilize various tools like Spark and MLlib along with
GeoPandas and the United States GeoJSON dataset to train a model that has an accuracy of 80% and a precision of 85%. The prediction results of the model are displayed in a Tableau workbook which has seen success in predicting floods that have occurred in the real world. We believe that this application can be used as another tool alongside existing technologies for flood detection and prevention. Our application will be able to support and validate findings from existing tools, which will result in a more thorough check for floods. This can hopefully prompt more preemptive action from various counties across the United States to mitigate damages from flooding.

[Read the paper](https://github.com/satyajeetmaharana/floodprediction/blob/master/Flood_Prediction_Final_Paper.pdf)

## Datasets

<p style="display: flex;justify-content: space-between;">
Source 1 : The first dataset considered is the NCDC Storm Events dataset. The dataset covers the personal injuries and damage estimates from storms that affected areas in the U.S. The data will be filtered for flood specific events by the timing of each flood, followed by its location. This will give an idea of where and when floods in the U.S. have occurred in the past.

Source 2 : The second dataset used is the NOAA Daily Summaries. The dataset contains daily weather data within the U.S. since 1763. This can be a source to look at many key metrics relating to flooding at specific times in the United States like a region’s rainfall, temperature, wind speeds. These metrics are used alongside the other datasets to confirm and compare against when flooding occurred and how much rainfall occurred at the time.

Source 3 : The last dataset used is the NOAA Precipitation Reconstruction. This dataset is a monthly precipitation analysis on a 2.5 degree latitude by 2.5 degree longitude global grid since 1948. The data is collected globally from gauge observations at 17,000 stations for the land portion and historical gauge observations over islands and land areas for the oceanic portion. This dataset is vital for comparing how much precipitation occurred when floods did happen, which is derived from the NCDC Storm Events dataset. This dataset is updated quasi real-time at NOAA/CPC.
</p>

![Flood prediction design diagram](https://user-images.githubusercontent.com/37962353/58741462-d0c81100-83e6-11e9-8e95-732f361ce157.png)
  
  
## Application Design

![App Diagram](https://user-images.githubusercontent.com/37962353/58741461-d0c81100-83e6-11e9-84e8-5b445fb615bc.png)
