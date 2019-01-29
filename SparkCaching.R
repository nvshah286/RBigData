library(sparklyr)
library(dplyr)
library(ggplot2)

# Install Spark version 2
# Customize the connection configuration
conf <- spark_config()
conf$`sparklyr.shell.driver-memory` <- "2G"

# Connect to Spark
sc <- spark_connect(master = "local", config = conf)

## reading the file into spark cluster

# memory = FALSE will only map the file with spark but will not create RDD in the memory
# This makes read command run faster but any subsequent data transformations will take longer 
spark_read_csv(sc, "flights_spark_2008", 
               "/Users/nvs/Downloads/2008.csv.bz2", memory = FALSE)

spark_read_csv(sc, "flights_spark_2008_inmem", 
               "/Users/nvs/Downloads/2008.csv.bz2", memory = TRUE)

## lazy transform. This execution will be very slow since data is not read into Spark memory or R.
# we just have the mapping from the previous statement (with memory = FALSE)

flights_table <- tbl(sc,"flights_spark_2008") %>%
  mutate(DepDelay = as.numeric(DepDelay),
         ArrDelay = as.numeric(ArrDelay),
         DepDelay > 15 , DepDelay < 240,
         ArrDelay > -60 , ArrDelay < 360, 
         Gain = DepDelay - ArrDelay) %>%
  filter(ArrDelay > 0) %>%
  select(Origin, Dest, UniqueCarrier, Distance, DepDelay, ArrDelay, Gain)

## storing the result in spark 
sdf_register(flights_table, "flights_spark")

## explictly caching data in memory
tbl_cache(sc, "flights_spark")


spark_read_csv(sc, "flights_spark_2007" , "/Users/nvs/Downloads/2007.csv.bz2", memory = FALSE)


fs_2008 = tbl(sc, "flights_spark_2008")
fs_2007 = tbl(sc, "flights_spark_2007")

all_flights <- fs_2008 %>% dplyr::union(fs_2007) %>%
  group_by(Year, Month) %>%
  tally()

all_flights <- all_flights %>%
  collect()

ggplot(data = all_flights, aes(x = Month, y = n/1000, fill = factor(Year))) +
  geom_area(position = "dodge", alpha = 0.5) +
  geom_line(alpha = 0.4) +
  scale_fill_brewer(palette = "Dark2", name = "Year") +
  scale_x_continuous(breaks = 1:12, labels = c("J","F","M","A","M","J","J","A","S","O","N","D")) +
  theme_light() +
  labs(y="Number of Flights (Thousands)", title = "Number of Flights Year-Over-Year")

