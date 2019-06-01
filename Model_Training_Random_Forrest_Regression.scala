//Before running the ML lib models, we had to convert our CSV file to the required LibSVM file first. 


import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}

//val data = sqlContext.read.format("libsvm").load("/user/sm8235/random_1/no_idea_lib_new.txt")

val data = sqlContext.read.format("libsvm").load("/user/sm8235/final_code/floodPrediction_dataset_7pm_libsvm.txt")

val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(data)

val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

val rf = new RandomForestRegressor().setLabelCol("label").setFeaturesCol("indexedFeatures").setNumTrees(25)

val pipeline = new Pipeline().setStages(Array(featureIndexer, rf))

val model = pipeline.fit(trainingData)

val predictions = model.transform(testData)

predictions.select("prediction", "label", "features").show(5)


val rootmeansquared_evaluator = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse")
val precision_evaluator = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("precision")
val recall_evaluator = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("recall")
val f1_evaluator = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("f1")

val rootmeansquared = rootmeansquared_evaluator.evaluate(predictions)
val precision = precision_evaluator.evaluate(predictions)
val recall = recall_evaluator.evaluate(predictions)
val f1_score = f1_evaluator.evaluate(predictions)


println(s"Root Mean Squared Error = $rootmeansquared")
println(s"precision = $precision")
println(s"recall= $recall")
println(s"f1 Score = $f1_score")


val rfModel = model.stages(1).asInstanceOf[RandomForestRegressionModel]
println(s"Learned regression forest model:\n ${rfModel.toDebugString}")



/******* IGNORE REMEDIATION PART ***************/


val (min_pred, max_pred) = predictions.select(min("prediction"), max("prediction")).as[(Double, Double)].first()


val preds_new = predictions.withColumn("prediction_normalized", ($"prediction" - min_pred) / (max_pred-min_pred))


sqlContext.sql("""SELECT COUNT(*) FROM preds WHERE prediction_normalized > 0.31""").show()






//val predictions_new = predictions.withColumn("PredictionNew", when(col("prediction_normalized") >0.31,"1").otherwise("0"))





val remediation_data = sqlContext.read.format("libsvm").load("/user/sm8235/final_code/DATA_TEST_LIBSVM.txt")


val remediation_predictions = model.transform(remediation_data)

val (min_rem_pred, max_rem_pred) = remediation_predictions.select(min("prediction"), max("prediction")).as[(Double, Double)].first()


val preds_new = remediation_predictions.withColumn("prediction_normalized", ($"prediction" - min_rem_pred) / (max_rem_pred-min_rem_pred))


preds_new.registerTempTable("rem_preds")



preds_new.write.format("com.databricks.spark.csv").option("header","true").save("/user/sm8235/final_code/tableau_data.csv")
val df_rem = sqlContext.sql("""SELECT prediction_normalized FROM rem_preds""")



///user/sm8235/final_code/DATA_FOR_LIB.csv

val df_rem_to_be_merged = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("dateFormat","YYYY-MM-DD").option("inferSchema","true").load("/user/sm8235/final_code/DATA_FOR_TABLEAU.csv")




//val dfs = Seq(df_rem, df_rem_to_be_merged)



df_rem.write.format("com.databricks.spark.csv").option("header","true").save("/user/sm8235/final_code/only_preds.csv")



//hdfs dfs -getmerge /user/sm8235/final_code/only_preds.csv only_preds.csv




//scp sm8235@dumbo.es.its.nyu.edu:/home/sm8235/DATA_FOR_TABLEAU.csv DATA_FOR_TABLEAU.csv


//scp sm8235@dumbo.es.its.nyu.edu:/home/sm8235/only_preds.csv only_preds.csv
