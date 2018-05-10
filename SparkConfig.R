# setting up a local connection with spark
sc <- spark_connect(master="local")

# read the config of the default local connection
conf <- spark_config()

conf


## update some params.
conf$`sparklyr.cores.local` <- 4
conf$`sparklyr.shell.driver-memory` <- "1G"
conf$spark.memory.fraction <- 0.9
conf$spark.executor.cores <- 4
sc <- spark_connect(master="local",
                    config = conf)
conf2 <- spark_config()
conf2
