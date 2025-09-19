# Project_BigData_Spark_and_R

📌 **General description:**  
In this project, I set out to explore and analyze the ["Walmart Recruiting - Store Sales Forecasting"](https://www.kaggle.com/c/walmart-recruiting-store-sales-forecasting/data) dataset available on the Kaggle website, using the R programming language and Spark (run locally).  

📂 The walmart dataset contains detailed information about Walmart stores, their weekly sales, and associated features.   
The dataset is structured in three main files: **"stores.csv", "train.csv" and "features.csv"**.    
Short description of each dataset:
 + **stores.csv** - includes information about the 45 stores, indicating the type and size of the store.  
 + **train.csv** - contains the historical data of each department's weekly sales: Store, Dept, Date, Weekly_Sales (sales for the respective department in the given store), IsHoliday.  
 + **features.csv** - contains additional features related to the store: Store, Date, Temperature, Fuel_Price, MarkDown1-5, IPC, Unemployment, IsHoliday (if the week is a special holiday week).

🎯 **Project objective**:  
The main objective was to identify and evaluate the factors that have a significant influence on weekly sales within each Walmart store.  
I set out to build a predictive model that would help estimate these sales, based on the variables identified as relevant.

🗂️ **Project structure and plan**:  
To achieve this objective, I implemented and compared several regression models, using **Spark (via the sparklyr package)** for data processing, model training, and evaluation, and **R** for data exploration, visualization, and interpretation of the results.  
 I analyzed and compared the performance of the models in two scenarios:
* without cross-validation
* with cross-validation

📂 **Project steps:**  
1. **Data Loading and Exploration**
  - Loading data into Spark using sparklyr.  
  - Exploring data with familiar R functions for visualization and descriptive statistics: ggplot2, tidyverse, dplyr.  
2. **Data Cleaning and Preprocessing data**  using sparklyr functions alongside tidyverse tools.  
3. **Building and training regression models:**  
   - Linear Regression → ml_linear_regression()  
   - Random Forest → ml_random_forest_regressor()  
   - Gradient Boosted Trees → ml_gradient_boosted_trees()  
   - Decision Tree Regression → ml_decision_tree_regressor()
4. **Model Evaluation**
 - ***Metrics used***: **Root Mean Squared Error (RMSE) and R-squared (R²)**.  
 - ***Visualization of model performance through graphs**:
   + comparison of performance metrics
   + importance of variables for each model


## About Sparklyr:  
Sparklyr is an  opern-source R Package that provides a interface between R and Apache Spark. It enables R users to interact with Spark using familiar syntax R and data structures. Sparklyr allows seamless integration of Spark's distributed computing capabilities with R's rich ecosystem of packages for data analysis, visualization, and statistical modeling.  

## Sparklyr installation and setup:  
* Use the 'install.packages("sparklyr")' function in R to install sparklyr from CRAN. This will download and install the latest version of sparklyr and its dependencies.   
*	Install Java/MS packages. It can be downloaded by accessing this link. https://www.azul.com/downloads/?package=jdk#zulu  
* Install Spark  
* Setting up a Spark connection:  
  + Connecting to a local Spark. This typically involves specifying the Spark master URL and configuring any necessary environment variables.  
  + Or connecting to a remote Spark cluster. 


