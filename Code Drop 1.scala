scp stations.txt sm8235@dumbo.es.its.nyu.edu:/home/sm8235/
val stations = sc.textFile("/user/sm8235/stations/stations.txt")

val stations_split = stations.map(line => line.split("\\s+"))

val stations_int_float = stations_split.map(line => (line(0), line(1).toDouble, line(2).toDouble, line(4)))


val stations_filter = stations_int_float.filter(line => line._1.startsWith("US"))

val stations_final = stations_filter.map(line => line.mkString(","))

88.75N - 88.75S and 1.25E - 358.75E

ID            1-11   Character
LATITUDE     13-20   Real
LONGITUDE    22-30   Real
ELEVATION    32-37   Real
STATE        39-40   Character
NAME         42-71   Character
GSN FLAG     73-75   Character
HCN/CRN FLAG 77-79   Character
WMO ID       81-85   Character






scala> sc
res0: org.apache.spark.SparkContext = org.apache.spark.SparkContext@4131261

scala> val rainfall_data = sc.textFile("hdfs://dumbo/user/sp4887/bdad_project/rainfall_data")
rainfall_data: org.apache.spark.rdd.RDD[String] = hdfs://dumbo/user/sp4887/bdad_project/rainfall_data MapPartitionsRDD[1] at textFile at <console>:27

rainfall_data.take(5)
res25: Array[String] = Array(1948   1  -88.75    1.25 -999.00, 1948   1  -88.75    3.75 -999.00, 1948   1  -88.75    6.25 -999.00, 1948   1  -88.75    8.75 -999.00, 1948   1  -88.75   11.25 -999.00)

scala> rainfall_data.top(5)
res24: Array[String] = Array(2019  12  -88.75  358.75 -999.00, 2019  12  -88.75  356.25 -999.00, 2019  12  -88.75  353.75 -999.00, 2019  12  -88.75  351.25 -999.00, 2019  12  -88.75  348.75 -999.00)

scala> val rainfall_rdd = rainfall_data.map(line => line.split("\\s+"))
rainfall_rdd: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[2] at map at <console>:29

scala> rainfall_rdd.take(5)
res26: Array[Array[String]] = Array(Array(1948, 1, -88.75, 1.25, -999.00), Array(1948, 1, -88.75, 3.75, -999.00), Array(1948, 1, -88.75, 6.25, -999.00), Array(1948, 1, -88.75, 8.75, -999.00), Array(1948, 1, -88.75, 11.25, -999.00))

scala> val rainfall_rdd_int_float = rainfall_rdd.map(line => (line(0).toInt, line(1).toInt, line(2).toDouble, line(3).toDouble, line(4).toDouble))
rainfall_rdd_int_float: org.apache.spark.rdd.RDD[(Int, Int, Double, Double, Double)] = MapPartitionsRDD[14] at map at <console>:35

scala> rainfall_rdd_int_float.take(10)
res19: Array[(Int, Int, Double, Double, Double)] = Array((1948,1,-88.75,1.25,-999.0), (1948,1,-88.75,3.75,-999.0), (1948,1,-88.75,6.25,-999.0), (1948,1,-88.75,8.75,-999.0), (1948,1,-88.75,11.25,-999.0), (1948,1,-88.75,13.75,-999.0), (1948,1,-88.75,16.25,-999.0), (1948,1,-88.75,18.75,-999.0), (1948,1,-88.75,21.25,-999.0), (1948,1,-88.75,23.75,-999.0))

scala> val rainfall_filter = rainfall_rdd_int_float.filter(line => (line._3 <= -25.84 && line._3 >= -49.38 && line._4 >= 246.95 && line._4 <= 304.67))
rainfall_filter: org.apache.spark.rdd.RDD[(Int, Int, Double, Double, Double)] = MapPartitionsRDD[16] at filter at <console>:37

scala> rainfall_filter.take(10)
res20: Array[(Int, Int, Double, Double, Double)] = Array((1948,1,-48.75,248.75,-0.07), (1948,1,-48.75,251.25,-0.09), (1948,1,-48.75,253.75,-0.14), (1948,1,-48.75,256.25,-0.16), (1948,1,-48.75,258.75,-0.19), (1948,1,-48.75,261.25,-0.24), (1948,1,-48.75,263.75,-0.27), (1948,1,-48.75,266.25,-0.29), (1948,1,-48.75,268.75,-0.23), (1948,1,-48.75,271.25,-0.14))

scala> rainfall_filter.top(5)
res21: Array[(Int, Int, Double, Double, Double)] = Array((2019,12,-26.25,303.75,-999.0), (2019,12,-26.25,301.25,-999.0), (2019,12,-26.25,298.75,-999.0), (2019,12,-26.25,296.25,-999.0), (2019,12,-26.25,293.75,-999.0))

scala> rainfall_filter.saveAsTextFile("hdfs://dumbo/user/sp4887/bdad_project/filtered_rainfall_data")


==================================================================================
Note: Because the dataset contains the precipitation from all over the world (over both land and sea), the range is 88.75N - 88.75S and 1.25E - 358.75E, we need to filter it to only include the US region which is 66.95 W to 124.67 W (Right to left) and 49.38 N to 25.84 N (Top to bottom).
