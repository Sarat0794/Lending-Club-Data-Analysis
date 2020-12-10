# Databricks notebook source
# MAGIC %md
# MAGIC #Exploratory Data Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lc_loan_data

# COMMAND ----------

# Converting the table into DataFrame
lc_df = spark.table('lc_loan_data')
display(lc_df)

# COMMAND ----------

lc_df.show()

# COMMAND ----------

# Display function is specific to DataBricks.
display(lc_df.describe())

# COMMAND ----------

# MAGIC %sql
# MAGIC /*Total loan amount in each state*/
# MAGIC select addr_state, sum(loan_amnt) from lc_loan_data group by addr_state

# COMMAND ----------

# Number of loan applicants in each state. select any column for count. 
from pyspark.sql.functions import count, isnan, when, col, log
display(lc_df.groupBy('addr_state').agg(count('loan_amnt').alias('count')))

# COMMAND ----------

# MAGIC %sql
# MAGIC /*Number of bad loans in each state */
# MAGIC select addr_state, count(loan_amnt) from lc_loan_data where bad_loan='yes' group by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Check the distribution of customer loan across different grades */
# MAGIC select grade, sum(loan_amnt) from lc_loan_data group by grade

# COMMAND ----------

# MAGIC %sql
# MAGIC /* the amount of bad loan across each grade */
# MAGIC select grade, bad_loan,sum(loan_amnt) as total_loan_amnt from lc_loan_data group by grade, bad_loan

# COMMAND ----------

# MAGIC %md Univariate Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC /*Histogram to view the distribution of loan_amnt*/
# MAGIC select loan_amnt from lc_loan_data

# COMMAND ----------

# MAGIC %sql
# MAGIC /*Now taking boxplot to check for outliers in loan_amnt*/
# MAGIC select loan_amnt from lc_loan_data

# COMMAND ----------

# MAGIC %sql
# MAGIC /*Now taking boxplot to check for outliers in loan_amnt*/
# MAGIC select loan_amnt from lc_loan_data

# COMMAND ----------

display(lc_df.describe())

# COMMAND ----------

# Removing % from the interset_rate column.
# It is good to define a function when you know that you are going to use it more often. 
from pyspark.sql.types import FloatType
def trim(string):
  return string.strip("%")

# Now we have register this function as a spark udf function.
# By doing that you can call the function in a distributed way. i.e it will get registered in every executer and it will be available across your distributed mode. 
spark.udf.register("trimperct", trim)


# COMMAND ----------

# MAGIC %sql
# MAGIC select int_rate, cast(trimperct(int_rate) as float) as int_rate_float from lc_loan_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select bad_loan, cast(trimperct(int_rate) as float) as int_rate_float from lc_loan_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select grade,purpose,count(*) as count from lc_loan_data group by grade, purpose
# MAGIC /* We can observe that dept_consolidation is the major reason why people come for loan*/

# COMMAND ----------

# MAGIC %sql
# MAGIC select total_acc, count(*) as count from lc_loan_data group by 1 order by 1

# COMMAND ----------


