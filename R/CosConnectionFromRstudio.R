library(sparklyr)
library(dplyr)
# list Spark kernels
kernels <- list_spark_kernels()
kernels

# connect to Spark kernel
sc <- spark_connect(kernels[3])

#Get below credential generated from your notebook or from Bluemix, if you are reffering to bluemix, you need to copy the service id starting
#from iam

credentials <-list(
  endpoint = "https://s3-api.us-geo.objectstorage.service.networklayer.com",
  api.key = "XXXXXXXXXXXXX",
  service.id = "iam-ServiceId-2e40c3da-e144-4f98-b513-484b58c7e470",
  bucket.name = "catalogdsxreproduce4a77ab6a4f2f47b3b6bedc7174a64c4a",
  file.name = "cars.csv",
  iam.service.endpoint = "https://iam.ng.bluemix.net/oidc/token")


#Get spark context 
ctx <- sparklyr::spark_context(sc)
#Use below to set the java spark context 
jsc <- invoke_static( sc, "org.apache.spark.api.java.JavaSparkContext", "fromSparkContext", ctx )

hconf <- jsc %>% invoke("hadoopConfiguration")

hconf %>% invoke("set","fs.cos.myCos.iam.api.key", credentials["api.key"])
hconf %>% invoke("set","fs.cos.myCos.iam.service.id", credentials["service.id"])
hconf %>% invoke("set","fs.cos.myCos.endpoint", credentials["endpoint"])

#catalogdsxreproduce4a77ab6a4f2f47b3b6bedc7174a64c4a Bucket name
path <- "cos://catalogdsxreproduce4a77ab6a4f2f47b3b6bedc7174a64c4a.myCos/cars.csv"


#Lets try to read using sparklyr packages 
carscsv_tbl <- spark_read_csv(sc,name = "carscsvtlb",path = path)    

src_tbls(sc)
head(carscsv_tbl,4)

