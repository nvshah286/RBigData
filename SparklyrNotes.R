library(dplyr)
library(nycflights13)
library(ggplot2)
library(sparklyr)
library(microbenchmark)

flights2 = nycflights13::flights

sc <- spark_connect(master="local")
## sc is the spark context... a connection object to communicate between R & Spark cluster
str(sc)
class(sc)

##copying the data to spark instance from R .. not loading data from R memory
flights <- copy_to(sc, flights, "flights")
airlines <- copy_to(sc, airlines, "airlines")

## This will show the class and structrue of the spark data frame. its stored on local spark cluster
class(flights)
str(flights)


## loads the environment variables in spark.
src_tbls(sc)

## Note : dplyr functions can directly be used on spark data frames and tables. 

select(flights,  year, month,dep_delay)

flights %>% group_by(month) %>% summarize(avgDelay = mean(dep_delay, na.rm =T)) %>% arrange(month)

## checking performance of loadin data from spark vs R memory 
## even for a simple filter function, you can compare the performance again.(spark is 6 times faster on average.)
microbenchmark(flights %>% filter(dep_delay > 1000))
microbenchmark(flights2 %>% filter(dep_delay > 1000))



  