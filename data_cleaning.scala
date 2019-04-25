val df = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("dateFormat","YYYY-MM-DD").option("inferSchema", "true").load("/user/sm8235/NOAA_Daily.csv")

df.registerTempTable("NOAA_Daily")




val df_cleaned = sqlContext.sql("""SELECT STATION,DATE,PRCP ,SNOW ,SNWD ,TMAX ,TMIN ,ACMC ,ACMH ,ACSC ,ACSH ,AWND ,EVAP ,GAHT ,MDEV ,MDPR ,MDSF ,MDTN ,MDTX ,MDWM ,MNPN ,MXPN ,PGTM ,PSUN ,SN01,SN02,SN03,SN11,SN12,SN13,SN14,SN21,SN22,SN23,SN31,SN32,SN33,SN34,SN35,SN36,SN51,SN52,SN53,SN54,SN55,SN56,SN57,SN61,SN72,SN81,SN82,SN83,SNOW,SNWD,SX01,SX02,SX03,SX11,SX12,SX13,SX14,SX15,SX17,SX21,SX22,SX23,SX31,SX32,SX33,SX34,SX35,SX36,SX51,SX52,SX53,SX54,SX55,SX56,SX57,SX61,SX72,SX81,SX82,SX83,TAVG,THIC,TMAX,TMIN,TOBS,TSUN,WDF1,WDF2,WDF5,WDFG,WDFI,WDFM,WDMV,WESD,WESF,WSF1,WSF2,WSF5,WSFG,WSFI,WSFM,WT01,WT02,WT03,WT04,WT05,WT06,WT07,WT08,WT09,WT10,WT11,WT12,WT13,WT14,WT15,WT16,WT17,WT18,WT19,WT21,WT22,WV01,WV03,WV07,WV18,WV20 FROM NOAA_Daily """)






val schema = new StructType().add("STATION",StringType,true).add("DATE",DataTypes.DateType,true).add("PRCP ",DoubleType,true).add("SNOW ",DoubleType,true).add("SNWD ",DoubleType,true).add("TMAX ",DoubleType,true).add("TMIN ",DoubleType,true).add("ACMC ",DoubleType,true).add("ACMH ",DoubleType,true).add("ACSC ",DoubleType,true).add("ACSH ",DoubleType,true).add("AWND ",DoubleType,true).add("EVAP ",DoubleType,true).add("GAHT ",DoubleType,true).add("MDEV ",DoubleType,true).add("MDPR ",DoubleType,true).add("MDSF ",DoubleType,true).add("MDTN ",DoubleType,true).add("MDTX ",DoubleType,true).add("MDWM ",DoubleType,true).add("MNPN ",DoubleType,true).add("MXPN ",DoubleType,true).add("PGTM ",DoubleType,true).add("PSUN ",DoubleType,true).add("SN01",DoubleType,true).add("SN02",DoubleType,true).add("SN03",DoubleType,true).add("SN11",DoubleType,true).add("SN12",DoubleType,true).add("SN13",DoubleType,true).add("SN14",DoubleType,true).add("SN21",DoubleType,true).add("SN22",DoubleType,true).add("SN23",DoubleType,true).add("SN31",DoubleType,true).add("SN32",DoubleType,true).add("SN33",DoubleType,true).add("SN34",DoubleType,true).add("SN35",DoubleType,true).add("SN36",DoubleType,true).add("SN51",DoubleType,true).add("SN52",DoubleType,true).add("SN53",DoubleType,true).add("SN54",DoubleType,true).add("SN55",DoubleType,true).add("SN56",DoubleType,true).add("SN57",DoubleType,true).add("SN61",DoubleType,true).add("SN72",DoubleType,true).add("SN81",DoubleType,true).add("SN82",DoubleType,true).add("SN83",DoubleType,true).add("SNOW",DoubleType,true).add("SNWD",DoubleType,true).add("SX01",DoubleType,true).add("SX02",DoubleType,true).add("SX03",DoubleType,true).add("SX11",DoubleType,true).add("SX12",DoubleType,true).add("SX13",DoubleType,true).add("SX14",DoubleType,true).add("SX15",DoubleType,true).add("SX17",DoubleType,true).add("SX21",DoubleType,true).add("SX22",DoubleType,true).add("SX23",DoubleType,true).add("SX31",DoubleType,true).add("SX32",DoubleType,true).add("SX33",DoubleType,true).add("SX34",DoubleType,true).add("SX35",DoubleType,true).add("SX36",DoubleType,true).add("SX51",DoubleType,true).add("SX52",DoubleType,true).add("SX53",DoubleType,true).add("SX54",DoubleType,true).add("SX55",DoubleType,true).add("SX56",DoubleType,true).add("SX57",DoubleType,true).add("SX61",DoubleType,true).add("SX72",DoubleType,true).add("SX81",DoubleType,true).add("SX82",DoubleType,true).add("SX83",DoubleType,true).add("TAVG",DoubleType,true).add("THIC",DoubleType,true).add("TMAX",DoubleType,true).add("TMIN",DoubleType,true).add("TOBS",DoubleType,true).add("TSUN",DoubleType,true).add("WDF1",DoubleType,true).add("WDF2",DoubleType,true).add("WDF5",DoubleType,true).add("WDFG",DoubleType,true).add("WDFI",DoubleType,true).add("WDFM",DoubleType,true).add("WDMV",DoubleType,true).add("WESD",DoubleType,true).add("WESF",DoubleType,true).add("WSF1",DoubleType,true).add("WSF2",DoubleType,true).add("WSF5",DoubleType,true).add("WSFG",DoubleType,true).add("WSFI",DoubleType,true).add("WSFM",DoubleType,true).add("WT01",DoubleType,true).add("WT02",DoubleType,true).add("WT03",DoubleType,true).add("WT04",DoubleType,true).add("WT05",DoubleType,true).add("WT06",DoubleType,true).add("WT07",DoubleType,true).add("WT08",DoubleType,true).add("WT09",DoubleType,true).add("WT10",DoubleType,true).add("WT11",DoubleType,true).add("WT12",DoubleType,true).add("WT13",DoubleType,true).add("WT14",DoubleType,true).add("WT15",DoubleType,true).add("WT16",DoubleType,true).add("WT17",DoubleType,true).add("WT18",DoubleType,true).add("WT19",DoubleType,true).add("WT21",DoubleType,true).add("WT22",DoubleType,true).add("WV01",DoubleType,true).add("WV03",DoubleType,true).add("WV07",DoubleType,true).add("WV18",DoubleType,true).add("WV20",DoubleType,true)


val df = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("dateFormat","YYYY-MM-DD").schema(schema).load("/user/sm8235/NOAA_Daily.csv")


df_cleaned.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/user/sm8235/NOAA_Daily_cleaned.csv")



val schema = new StructType().add("STATION",StringType,true).add("DATE",DateType,true).add("PRCP ",DoubleType,true).add("SNOW ",DoubleType,true).add("SNWD ",DoubleType,true).add("TMAX ",DoubleType,true).add("TMIN ",DoubleType,true).add("ACMC ",DoubleType,true).add("ACMH ",DoubleType,true).add("ACSC ",DoubleType,true).add("ACSH ",DoubleType,true).add("AWND ",DoubleType,true).add("EVAP ",DoubleType,true).add("GAHT ",DoubleType,true).add("MDEV ",DoubleType,true).add("MDPR ",DoubleType,true).add("MDSF ",DoubleType,true).add("MDTN ",DoubleType,true).add("MDTX ",DoubleType,true).add("MDWM ",DoubleType,true).add("MNPN ",DoubleType,true).add("MXPN ",DoubleType,true).add("PGTM ",DoubleType,true).add("PSUN ",DoubleType,true).add("SN01",DoubleType,true).add("SN02",DoubleType,true).add("SN03",DoubleType,true).add("SN11",DoubleType,true).add("SN12",DoubleType,true).add("SN13",DoubleType,true).add("SN14",DoubleType,true).add("SN21",DoubleType,true).add("SN22",DoubleType,true).add("SN23",DoubleType,true).add("SN31",DoubleType,true).add("SN32",DoubleType,true).add("SN33",DoubleType,true).add("SN34",DoubleType,true).add("SN35",DoubleType,true).add("SN36",DoubleType,true).add("SN51",DoubleType,true).add("SN52",DoubleType,true).add("SN53",DoubleType,true).add("SN54",DoubleType,true).add("SN55",DoubleType,true).add("SN56",DoubleType,true).add("SN57",DoubleType,true).add("SN61",DoubleType,true).add("SN72",DoubleType,true).add("SN81",DoubleType,true).add("SN82",DoubleType,true).add("SN83",DoubleType,true).add("SNOW",DoubleType,true).add("SNWD",DoubleType,true).add("SX01",DoubleType,true).add("SX02",DoubleType,true).add("SX03",DoubleType,true).add("SX11",DoubleType,true).add("SX12",DoubleType,true).add("SX13",DoubleType,true).add("SX14",DoubleType,true).add("SX15",DoubleType,true).add("SX17",DoubleType,true).add("SX21",DoubleType,true).add("SX22",DoubleType,true).add("SX23",DoubleType,true).add("SX31",DoubleType,true).add("SX32",DoubleType,true).add("SX33",DoubleType,true).add("SX34",DoubleType,true).add("SX35",DoubleType,true).add("SX36",DoubleType,true).add("SX51",DoubleType,true).add("SX52",DoubleType,true).add("SX53",DoubleType,true).add("SX54",DoubleType,true).add("SX55",DoubleType,true).add("SX56",DoubleType,true).add("SX57",DoubleType,true).add("SX61",DoubleType,true).add("SX72",DoubleType,true).add("SX81",DoubleType,true).add("SX82",DoubleType,true).add("SX83",DoubleType,true).add("TAVG",DoubleType,true).add("THIC",DoubleType,true).add("TMAX",DoubleType,true).add("TMIN",DoubleType,true).add("TOBS",DoubleType,true).add("TSUN",DoubleType,true).add("WDF1",DoubleType,true).add("WDF2",DoubleType,true).add("WDF5",DoubleType,true).add("WDFG",DoubleType,true).add("WDFI",DoubleType,true).add("WDFM",DoubleType,true).add("WDMV",DoubleType,true).add("WESD",DoubleType,true).add("WESF",DoubleType,true).add("WSF1",DoubleType,true).add("WSF2",DoubleType,true).add("WSF5",DoubleType,true).add("WSFG",DoubleType,true).add("WSFI",DoubleType,true).add("WSFM",DoubleType,true).add("WT01",DoubleType,true).add("WT02",DoubleType,true).add("WT03",DoubleType,true).add("WT04",DoubleType,true).add("WT05",DoubleType,true).add("WT06",DoubleType,true).add("WT07",DoubleType,true).add("WT08",DoubleType,true).add("WT09",DoubleType,true).add("WT10",DoubleType,true).add("WT11",DoubleType,true).add("WT12",DoubleType,true).add("WT13",DoubleType,true).add("WT14",DoubleType,true).add("WT15",DoubleType,true).add("WT16",DoubleType,true).add("WT17",DoubleType,true).add("WT18",DoubleType,true).add("WT19",DoubleType,true).add("WT21",DoubleType,true).add("WT22",DoubleType,true).add("WV01",DoubleType,true).add("WV03",DoubleType,true).add("WV07",DoubleType,true).add("WV18",DoubleType,true).add("WV20",DoubleType,true)
val df = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("dateFormat","YYYY-MM-DD").option("nullValue", "null").schema(schema).load("/user/sm8235/NOAA_Daily_cleaned.csv")




df.na.fill(df.columns.zip(df.select(df.columns.map(mean(_)): array("PRCP","SNOW","SNWD","TMAX","TMIN","ACMC","ACMH","ACSC","ACSH","AWND","EVAP","GAHT","MDEV","MDPR","MDSF","MDTN","MDTX","MDWM","MNPN","MXPN","PGTM","PSUN","SN01","SN02","SN03","SN11","SN12","SN13","SN14","SN21","SN22","SN23","SN31","SN32","SN33","SN34","SN35","SN36","SN51","SN52","SN53","SN54","SN55","SN56","SN57","SN61","SN72","SN81","SN82","SN83","SNOW","SNWD","SX01","SX02","SX03","SX11","SX12","SX13","SX14","SX15","SX17","SX21","SX22","SX23","SX31","SX32","SX33","SX34","SX35","SX36","SX51","SX52","SX53","SX54","SX55","SX56","SX57","SX61","SX72","SX81","SX82","SX83","TAVG","THIC","TMAX","TMIN","TOBS","TSUN","WDF1","WDF2","WDF5","WDFG","WDFI","WDFM","WDMV","WESD","WESF","WSF1","WSF2","WSF5","WSFG","WSFI","WSFM","WT01","WT02","WT03","WT04","WT05","WT06","WT07","WT08","WT09","WT10","WT11","WT12","WT13","WT14","WT15","WT16","WT17","WT18","WT19","WT21","WT22","WV01","WV03","WV07","WV18","WV20"))).toMap)





array("PRCP","SNOW","SNWD","TMAX","TMIN","ACMC","ACMH","ACSC","ACSH","AWND","EVAP","GAHT","MDEV","MDPR","MDSF","MDTN","MDTX","MDWM","MNPN","MXPN","PGTM","PSUN","SN01","SN02","SN03","SN11","SN12","SN13","SN14","SN21","SN22","SN23","SN31","SN32","SN33","SN34","SN35","SN36","SN51","SN52","SN53","SN54","SN55","SN56","SN57","SN61","SN72","SN81","SN82","SN83","SNOW","SNWD","SX01","SX02","SX03","SX11","SX12","SX13","SX14","SX15","SX17","SX21","SX22","SX23","SX31","SX32","SX33","SX34","SX35","SX36","SX51","SX52","SX53","SX54","SX55","SX56","SX57","SX61","SX72","SX81","SX82","SX83","TAVG","THIC","TMAX","TMIN","TOBS","TSUN","WDF1","WDF2","WDF5","WDFG","WDFI","WDFM","WDMV","WESD","WESF","WSF1","WSF2","WSF5","WSFG","WSFI","WSFM","WT01","WT02","WT03","WT04","WT05","WT06","WT07","WT08","WT09","WT10","WT11","WT12","WT13","WT14","WT15","WT16","WT17","WT18","WT19","WT21","WT22","WV01","WV03","WV07","WV18","WV20")



df.na.fill(df.columns.zip(df.select(df.columns.map(mean(_)): array("PRCP","SNOW","SNWD","TMAX","TMIN","ACMC","ACMH","ACSC","ACSH","AWND","EVAP","GAHT","MDEV","MDPR","MDSF","MDTN","MDTX","MDWM","MNPN","MXPN","PGTM","PSUN","SN01","SN02","SN03","SN11","SN12","SN13","SN14","SN21","SN22","SN23","SN31","SN32","SN33","SN34","SN35","SN36","SN51","SN52","SN53","SN54","SN55","SN56","SN57","SN61","SN72","SN81","SN82","SN83","SNOW","SNWD","SX01","SX02","SX03","SX11","SX12","SX13","SX14","SX15","SX17","SX21","SX22","SX23","SX31","SX32","SX33","SX34","SX35","SX36","SX51","SX52","SX53","SX54","SX55","SX56","SX57","SX61","SX72","SX81","SX82","SX83","TAVG","THIC","TMAX","TMIN","TOBS","TSUN","WDF1","WDF2","WDF5","WDFG","WDFI","WDFM","WDMV","WESD","WESF","WSF1","WSF2","WSF5","WSFG","WSFI","WSFM","WT01","WT02","WT03","WT04","WT05","WT06","WT07","WT08","WT09","WT10","WT11","WT12","WT13","WT14","WT15","WT16","WT17","WT18","WT19","WT21","WT22","WV01","WV03","WV07","WV18","WV20")).first.toSeq).toMap)




df.select(df.columns.map(c => mean(when(c != "Date" && c != "STATION", col(c)))): _*).first.toSeq


if(c != "DATE") col(c)



df.columns.map(c => if(c != "DATE" && c !="STATION") col(c))


df.na.fill(df.columns.zip(df.select(df.columns.map(c => if(c != "DATE" && c !="STATION") mean(col(c))): _*).first.toSeq).toMap)








Array("PRCP","SNOW","SNWD","TMAX","TMIN","ACMC","ACMH","ACSC","ACSH","AWND","EVAP","GAHT","MDEV","MDPR","MDSF","MDTN","MDTX","MDWM","MNPN","MXPN","PGTM","PSUN","SN01","SN02","SN03","SN11","SN12","SN13","SN14","SN21","SN22","SN23","SN31","SN32","SN33","SN34","SN35","SN36","SN51","SN52","SN53","SN54","SN55","SN56","SN57","SN61","SN72","SN81","SN82","SN83","SNOW","SNWD","SX01","SX02","SX03","SX11","SX12","SX13","SX14","SX15","SX17","SX21","SX22","SX23","SX31","SX32","SX33","SX34","SX35","SX36","SX51","SX52","SX53","SX54","SX55","SX56","SX57","SX61","SX72","SX81","SX82","SX83","TAVG","THIC","TMAX","TMIN","TOBS","TSUN","WDF1","WDF2","WDF5","WDFG","WDFI","WDFM","WDMV","WESD","WESF","WSF1","WSF2","WSF5","WSFG","WSFI","WSFM","WT01","WT02","WT03","WT04","WT05","WT06","WT07","WT08","WT09","WT10","WT11","WT12","WT13","WT14","WT15","WT16","WT17","WT18","WT19","WT21","WT22","WV01","WV03","WV07","WV18","WV20")




val imputer = new Imputer().setInputCols(Array("PRCP","SNOW","SNWD","TMAX","TMIN","ACMC","ACMH","ACSC","ACSH","AWND","EVAP","GAHT","MDEV","MDPR","MDSF","MDTN","MDTX","MDWM","MNPN","MXPN","PGTM","PSUN","SN01","SN02","SN03","SN11","SN12","SN13","SN14","SN21","SN22","SN23","SN31","SN32","SN33","SN34","SN35","SN36","SN51","SN52","SN53","SN54","SN55","SN56","SN57","SN61","SN72","SN81","SN82","SN83","SNOW","SNWD","SX01","SX02","SX03","SX11","SX12","SX13","SX14","SX15","SX17","SX21","SX22","SX23","SX31","SX32","SX33","SX34","SX35","SX36","SX51","SX52","SX53","SX54","SX55","SX56","SX57","SX61","SX72","SX81","SX82","SX83","TAVG","THIC","TMAX","TMIN","TOBS","TSUN","WDF1","WDF2","WDF5","WDFG","WDFI","WDFM","WDMV","WESD","WESF","WSF1","WSF2","WSF5","WSFG","WSFI","WSFM","WT01","WT02","WT03","WT04","WT05","WT06","WT07","WT08","WT09","WT10","WT11","WT12","WT13","WT14","WT15","WT16","WT17","WT18","WT19","WT21","WT22","WV01","WV03","WV07","WV18","WV20")).setOutputCols(df.columns.map(c => s"${c}_imputed")).setStrategy("mean")

imputer.fit(df).transform(df)




val imputer = new Imputer().setInputCols(Array("PRCP ","SNOW ","SNWD ","TMAX ","TMIN ","ACMC ","ACMH ","ACSC ","ACSH ","AWND ","EVAP ","GAHT ","MDEV ","MDPR ","MDSF ","MDWM ","MNPN ","MXPN ","PGTM ","PSUN ","SN01","SN02","SN03","SN11","SN12","SN13","SN14","SN21","SN22","SN23","SN31","SN32","SN33","SN34","SN35","SN36","SN51","SN52","SN53","SN54","SN55","SN56","SN57","SN61","SN72","SN81","SN82","SN83","SNOW","SNWD","SX01","SX02","SX03","SX11","SX12","SX13","SX14","SX15","SX17","SX21","SX22","SX23","SX31","SX32","SX33","SX34","SX35","SX36","SX51","SX52","SX53","SX54","SX55","SX56","SX57","SX61","SX72","SX81","SX82","SX83","TAVG","THIC","TMAX","TMIN","TOBS","TSUN","WDF1","WDF2","WDF5","WDFG","WDFI","WDFM","WDMV","WESD","WESF","WSF1","WSF2","WSF5","WSFG","WSFI","WSFM","WT01","WT02","WT03","WT04","WT05","WT06","WT07","WT08","WT09","WT10","WT11","WT12","WT13","WT14","WT15","WT16","WT17","WT18","WT19","WT21","WT22","WV01","WV03","WV07","WV18","WV20")).setOutputCols(Array("impute_PRCP ","impute_SNOW ","impute_SNWD ","impute_TMAX ","impute_TMIN ","impute_ACMC ","impute_ACMH ","impute_ACSC ","impute_ACSH ","impute_AWND ","impute_EVAP ","impute_GAHT ","impute_MDEV ","impute_MDPR ","impute_MDSF ","impute_MDWM ","impute_MNPN ","impute_MXPN ","impute_PGTM ","impute_PSUN ","impute_SN01","impute_SN02","impute_SN03","impute_SN11","impute_SN12","impute_SN13","impute_SN14","impute_SN21","impute_SN22","impute_SN23","impute_SN31","impute_SN32","impute_SN33","impute_SN34","impute_SN35","impute_SN36","impute_SN51","impute_SN52","impute_SN53","impute_SN54","impute_SN55","impute_SN56","impute_SN57","impute_SN61","impute_SN72","impute_SN81","impute_SN82","impute_SN83","impute_SNOW","impute_SNWD","impute_SX01","impute_SX02","impute_SX03","impute_SX11","impute_SX12","impute_SX13","impute_SX14","impute_SX15","impute_SX17","impute_SX21","impute_SX22","impute_SX23","impute_SX31","impute_SX32","impute_SX33","impute_SX34","impute_SX35","impute_SX36","impute_SX51","impute_SX52","impute_SX53","impute_SX54","impute_SX55","impute_SX56","impute_SX57","impute_SX61","impute_SX72","impute_SX81","impute_SX82","impute_SX83","impute_TAVG","impute_THIC","impute_TMAX","impute_TMIN","impute_TOBS","impute_TSUN","impute_WDF1","impute_WDF2","impute_WDF5","impute_WDFG","impute_WDFI","impute_WDFM","impute_WDMV","impute_WESD","impute_WESF","impute_WSF1","impute_WSF2","impute_WSF5","impute_WSFG","impute_WSFI","impute_WSFM","impute_WT01","impute_WT02","impute_WT03","impute_WT04","impute_WT05","impute_WT06","impute_WT07","impute_WT08","impute_WT09","impute_WT10","impute_WT11","impute_WT12","impute_WT13","impute_WT14","impute_WT15","impute_WT16","impute_WT17","impute_WT18","impute_WT19","impute_WT21","impute_WT22","impute_WV01","impute_WV03","impute_WV07","impute_WV18","impute_WV20")).setStrategy("mean")


imputer.fit(df_new).transform(df_new)

val df_new = df.drop(df.col("MDTN ")).drop(df.col("MDTX "))



imputer.fit(df_new).transform(df_new)



impute_PRCP ,impute_SNOW ,impute_SNWD ,impute_TMAX ,impute_TMIN ,impute_ACMC ,impute_ACMH ,impute_ACSC ,impute_ACSH ,impute_AWND ,impute_EVAP ,impute_GAHT ,impute_MDEV ,impute_MDPR ,impute_MDSF ,impute_MDWM ,impute_MNPN ,impute_MXPN ,impute_PGTM ,impute_PSUN ,impute_SN01,impute_SN02,impute_SN03,impute_SN11,impute_SN12,impute_SN13,impute_SN14,impute_SN21,impute_SN22,impute_SN23,impute_SN31,impute_SN32,impute_SN33,impute_SN34,impute_SN35,impute_SN36,impute_SN51,impute_SN52,impute_SN53,impute_SN54,impute_SN55,impute_SN56,impute_SN57,impute_SN61,impute_SN72,impute_SN81,impute_SN82,impute_SN83,impute_SNOW,impute_SNWD,impute_SX01,impute_SX02,impute_SX03,impute_SX11,impute_SX12,impute_SX13,impute_SX14,impute_SX15,impute_SX17,impute_SX21,impute_SX22,impute_SX23,impute_SX31,impute_SX32,impute_SX33,impute_SX34,impute_SX35,impute_SX36,impute_SX51,impute_SX52,impute_SX53,impute_SX54,impute_SX55,impute_SX56,impute_SX57,impute_SX61,impute_SX72,impute_SX81,impute_SX82,impute_SX83,impute_TAVG,impute_THIC,impute_TMAX,impute_TMIN,impute_TOBS,impute_TSUN,impute_WDF1,impute_WDF2,impute_WDF5,impute_WDFG,impute_WDFI,impute_WDFM,impute_WDMV,impute_WESD,impute_WESF,impute_WSF1,impute_WSF2,impute_WSF5,impute_WSFG,impute_WSFI,impute_WSFM,impute_WT01,impute_WT02,impute_WT03,impute_WT04,impute_WT05,impute_WT06,impute_WT07,impute_WT08,impute_WT09,impute_WT10,impute_WT11,impute_WT12,impute_WT13,impute_WT14,impute_WT15,impute_WT16,impute_WT17,impute_WT18,impute_WT19,impute_WT21,impute_WT22,impute_WV01,impute_WV03,impute_WV07,impute_WV18,impute_WV20


val df_final = imputer.fit(df_new).transform(df_new)
dfRenamed.registerTempTable("NOAA_FINAL")


val df_complete = sqlContext.sql("""SELECT STATION,DATE,impute_PRCP,impute_SNOW,impute_SNWD,impute_TMAX,impute_TMIN ,impute_ACMC ,impute_ACMH ,impute_ACSC ,impute_ACSH ,impute_AWND ,impute_EVAP ,impute_GAHT ,impute_MDEV ,impute_MDPR ,impute_MDSF ,impute_MDWM ,impute_MNPN ,impute_MXPN ,impute_PGTM ,impute_PSUN ,impute_SN01,impute_SN02,impute_SN03,impute_SN11,impute_SN12,impute_SN13,impute_SN14,impute_SN21,impute_SN22,impute_SN23,impute_SN31,impute_SN32,impute_SN33,impute_SN34,impute_SN35,impute_SN36,impute_SN51,impute_SN52,impute_SN53,impute_SN54,impute_SN55,impute_SN56,impute_SN57,impute_SN61,impute_SN72,impute_SN81,impute_SN82,impute_SN83,impute_SNOW,impute_SNWD,impute_SX01,impute_SX02,impute_SX03,impute_SX11,impute_SX12,impute_SX13,impute_SX14,impute_SX15,impute_SX17,impute_SX21,impute_SX22,impute_SX23,impute_SX31,impute_SX32,impute_SX33,impute_SX34,impute_SX35,impute_SX36,impute_SX51,impute_SX52,impute_SX53,impute_SX54,impute_SX55,impute_SX56,impute_SX57,impute_SX61,impute_SX72,impute_SX81,impute_SX82,impute_SX83,impute_TAVG,impute_THIC,impute_TMAX,impute_TMIN,impute_TOBS,impute_TSUN,impute_WDF1,impute_WDF2,impute_WDF5,impute_WDFG,impute_WDFI,impute_WDFM,impute_WDMV,impute_WESD,impute_WESF,impute_WSF1,impute_WSF2,impute_WSF5,impute_WSFG,impute_WSFI,impute_WSFM,impute_WT01,impute_WT02,impute_WT03,impute_WT04,impute_WT05,impute_WT06,impute_WT07,impute_WT08,impute_WT09,impute_WT10,impute_WT11,impute_WT12,impute_WT13,impute_WT14,impute_WT15,impute_WT16,impute_WT17,impute_WT18,impute_WT19,impute_WT21,impute_WT22,impute_WV01,impute_WV03,impute_WV07,impute_WV18,impute_WV20 FROM NOAA_FINAL """)





val df_complete = sqlContext.sql("""SELECT STATION,DATE,impute_PRCP,impute_SNOW,impute_SNWD,impute_TMAX,impute_TMIN,impute_ACMC,impute_ACMH,impute_ACSC,impute_ACSH,impute_AWND,impute_EVAP,impute_GAHT,impute_MDEV,impute_MDPR,impute_MDSF,impute_MDWM,impute_MNPN,impute_MXPN,impute_PGTM,impute_PSUN,impute_SN01,impute_SN02,impute_SN03,impute_SN11,impute_SN12,impute_SN13,impute_SN14,impute_SN21,impute_SN22,impute_SN23,impute_SN31,impute_SN32,impute_SN33,impute_SN34,impute_SN35,impute_SN36,impute_SN51,impute_SN52,impute_SN53,impute_SN54,impute_SN55,impute_SN56,impute_SN57,impute_SN61,impute_SN72,impute_SN81,impute_SN82,impute_SN83,impute_SX01,impute_SX02,impute_SX03,impute_SX11,impute_SX12,impute_SX13,impute_SX14,impute_SX15,impute_SX17,impute_SX21,impute_SX22,impute_SX23,impute_SX31,impute_SX32,impute_SX33,impute_SX34,impute_SX35,impute_SX36,impute_SX51,impute_SX52,impute_SX53,impute_SX54,impute_SX55,impute_SX56,impute_SX57,impute_SX61,impute_SX72,impute_SX81,impute_SX82,impute_SX83,impute_TAVG,impute_THIC,impute_TOBS,impute_TSUN,impute_WDF1,impute_WDF2,impute_WDF5,impute_WDFG,impute_WDFI,impute_WDFM,impute_WDMV,impute_WESD,impute_WESF,impute_WSF1,impute_WSF2,impute_WSF5,impute_WSFG,impute_WSFI,impute_WSFM,impute_WT01,impute_WT02,impute_WT03,impute_WT04,impute_WT05,impute_WT06,impute_WT07,impute_WT08,impute_WT09,impute_WT10,impute_WT11,impute_WT12,impute_WT13,impute_WT14,impute_WT15,impute_WT16,impute_WT17,impute_WT18,impute_WT19,impute_WT21,impute_WT22,impute_WV01,impute_WV03,impute_WV07,impute_WV18,impute_WV20 FROM NOAA_FINAL """)





val dfRenamed = df_final.toDF(newNames: _*)







df_complete.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/user/sm8235/NOAA_Complete.csv")         







val newNames = Seq("STATION", "DATE","PRCP","SNOW","SNWD","TMAX","TMIN","ACMC","ACMH","ACSC","ACSH","AWND","EVAP","GAHT","MDEV","MDPR","MDSF","MDWM","MNPN","MXPN","PGTM","PSUN","SN01","SN02","SN03","SN11","SN12","SN13","SN14","SN21","SN22","SN23","SN31","SN32","SN33","SN34","SN35","SN36","SN51","SN52","SN53","SN54","SN55","SN56","SN57","SN61","SN72","SN81","SN82","SN83","SNOW","SNWD","SX01","SX02","SX03","SX11","SX12","SX13","SX14","SX15","SX17","SX21","SX22","SX23","SX31","SX32","SX33","SX34","SX35","SX36","SX51","SX52","SX53","SX54","SX55","SX56","SX57","SX61","SX72","SX81","SX82","SX83","TAVG","THIC","TMAX","TMIN","TOBS","TSUN","WDF1","WDF2","WDF5","WDFG","WDFI","WDFM","WDMV","WESD","WESF","WSF1","WSF2","WSF5","WSFG","WSFI","WSFM","WT01","WT02","WT03","WT04","WT05","WT06","WT07","WT08","WT09","WT10","WT11","WT12","WT13","WT14","WT15","WT16","WT17","WT18","WT19","WT21","WT22","WV01","WV03","WV07","WV18","WV20","impute_PRCP","impute_SNOW","impute_SNWD","impute_TMAX","impute_TMIN","impute_ACMC","impute_ACMH","impute_ACSC","impute_ACSH","impute_AWND","impute_EVAP","impute_GAHT","impute_MDEV","impute_MDPR","impute_MDSF","impute_MDWM","impute_MNPN","impute_MXPN","impute_PGTM","impute_PSUN","impute_SN01","impute_SN02","impute_SN03","impute_SN11","impute_SN12","impute_SN13","impute_SN14","impute_SN21","impute_SN22","impute_SN23","impute_SN31","impute_SN32","impute_SN33","impute_SN34","impute_SN35","impute_SN36","impute_SN51","impute_SN52","impute_SN53","impute_SN54","impute_SN55","impute_SN56","impute_SN57","impute_SN61","impute_SN72","impute_SN81","impute_SN82","impute_SN83","impute_SNOW","impute_SNWD","impute_SX01","impute_SX02","impute_SX03","impute_SX11","impute_SX12","impute_SX13","impute_SX14","impute_SX15","impute_SX17","impute_SX21","impute_SX22","impute_SX23","impute_SX31","impute_SX32","impute_SX33","impute_SX34","impute_SX35","impute_SX36","impute_SX51","impute_SX52","impute_SX53","impute_SX54","impute_SX55","impute_SX56","impute_SX57","impute_SX61","impute_SX72","impute_SX81","impute_SX82","impute_SX83","impute_TAVG","impute_THIC","impute_TMAX","impute_TMIN","impute_TOBS","impute_TSUN","impute_WDF1","impute_WDF2","impute_WDF5","impute_WDFG","impute_WDFI","impute_WDFM","impute_WDMV","impute_WESD","impute_WESF","impute_WSF1","impute_WSF2","impute_WSF5","impute_WSFG","impute_WSFI","impute_WSFM","impute_WT01","impute_WT02","impute_WT03","impute_WT04","impute_WT05","impute_WT06","impute_WT07","impute_WT08","impute_WT09","impute_WT10","impute_WT11","impute_WT12","impute_WT13","impute_WT14","impute_WT15","impute_WT16","impute_WT17","impute_WT18","impute_WT19","impute_WT21","impute_WT22","impute_WV01","impute_WV03","impute_WV07","impute_WV18","impute_WV20")














val df = sqlCtx.read.format("magellan").load("/user/sm8235/states_21basic")
  








val df_us_county = sqlContext.read.format("magellan").load("/user/sm8235/states_21basic")


df_us_county.select(explode($"metadata").as(Seq("k", "v"))).show(5)
