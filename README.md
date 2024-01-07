# Store Visitors Forecasting Regression Model

## Overview

This repository contains code for building a regression model to forecast daily store visitors using Snowflake, HEX, and Anaconda Python packages. The process involves creating a dataset, loading data into Snowflake, connecting HEX workbook with Snowflake, and building a regression model for daily visitors forecasting.

## Steps to Reproduce

### 1. Create a Dataset
   - Create a dataset containing historical daily store visitor data.

### 2. Create an Internal Stage and Upload Files
   - Create an internal stage in Snowflake.
   - Upload the dataset files to the internal stage.

### 3. Load Data into Table from Stage
   - Load the data from the internal stage into a Snowflake table.

### 4. Enable Anaconda Python Package
   - Ensure that Anaconda Python is installed and accessible in your environment.

### 5. Create a HEX Account and Connect with Snowflake
   - Create a HEX account.
   - Connect HEX workbook with Snowflake by creating a data source.

### 6. Import Necessary Libraries
   - Install and import the necessary Python libraries for data processing and modeling.

### 7. Create Snowpark Session to Connect HEX Workbook with Snowflake
   - Establish a Snowpark session to connect the HEX workbook with Snowflake.

### 8. Import Daily Visitors Historical Data as a Snowpark DataFrame
   - Import the historical data from Snowflake into a Snowpark DataFrame.

### 9. Split the Dataset into Training and Test DataFrames
   - Split the dataset into training and test DataFrames based on date for time series analysis.

### 10. Categorize Columns & Create Pipeline
   - Categorize columns as needed.
   - Create a data processing pipeline for the regression model.

### 11. Train the Model and Check Accuracy using R2
   - Train the regression model and evaluate its accuracy using R-squared.

### 12. Convert Pipeline to Sklearn File and Save it into Snowflake Stage
   - Convert the data processing pipeline to a sklearn file.
   - Save the sklearn file into a Snowflake stage.

### 13. Create Vectorized UDF for Batch Inference
   - Create a function to load the trained pipeline.
   - Develop a vectorized UDF for forecasting using the trained model.

### 14. Call UDF to Forecast Daily Visitors
   - Apply the UDF to forecast daily visitors for the `DAILY_VISITORS_NEW` table.
   - Save the forecast results into a Snowflake table.

## Contributors
   - List contributors and their roles in the project.

## License
   - Specify the license under which this project is distributed.

