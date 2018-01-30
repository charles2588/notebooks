install.packages('RPostgreSQL')

#Now make grab the connection credentials from (Host/port) from Destination settings from Secure Gateway since your
#onprem host/port is mapped to secure gateway destination.
#https://medium.com/ibm-data-science-experience/working-with-on-premises-databases-step-by-step-badfe5a60136
#Now also grab the username and password, databasename from your onprem system and any other SSL or other parameters you need

# Connecting to RPostgreSQL

library('RPostgreSQL')
drv <- dbDriver('PostgreSQL')  
db <- 'postgres'  
host_db <- 'cap-sg-prd-2.integration.ibmcloud.com'  
db_port <- '17538'  
db_user <- 'postgres'  
db_password <- 'XXXXX'

conn <- dbConnect(drv, dbname=db, host=host_db, port=db_port, user=db_user, password=db_password)

#Lets write some sample data table
data('mtcars')  
my_data <- data.frame(carname = rownames(mtcars), mtcars, row.names = NULL)  
my_data$carname <- as.character(my_data$carname)  
rm(mtcars)  

dbWriteTable(conn, name='cars', value=my_data)  

#Lets try to read it back
carQuery <- dbSendQuery(conn, 'select carname, cyl, gear from cars where cyl >= 6 and gear >= 5;')  
result <- fetch(carQuery)  
dbClearResult(carQuery)  
result
