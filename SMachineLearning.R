library(microbenchmark)
library(ggplot2)
library(dplyr)

sc <- spark_connect(master = "local")


iris_tbl <- copy_to(sc, iris, "iris", overwrite = TRUE)
iris_tbl

f1 = function(x){ ml_linear_regression(x = iris_tbl, formula = Sepal_Width ~ Sepal_Length) }
f2 = function(x){lm(Sepal.Width ~Sepal.Length, data = iris)}

microbenchmark(f1(),f2(), times = 10)

## trianing and testing data frames 
partitions <- iris_tbl %>%
  sdf_partition(training = 0.75, test = 0.25, seed = 1099)

fit <- partitions$training %>%
  ml_linear_regression(Petal_Length ~ Petal_Width)

estimate_mse <- function(df){
  sdf_predict(fit, df) %>%
    mutate(resid = Petal_Length - prediction) %>%
    summarize(mse = mean(resid ^ 2)) %>%
    collect
}

sapply(partitions, estimate_mse)
