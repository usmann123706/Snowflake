# Snowpark for Python
from snowflake.snowpark.functions import udf
from snowflake.snowpark import types as T
import snowflake.snowpark.functions as F
# Snowpark ML
from snowflake.ml.modeling.xgboost import XGBRegressor
from snowflake.ml.modeling.pipeline import Pipeline
from snowflake.ml.modeling.preprocessing import OrdinalEncoder
# data science libs
import pandas as pd
import numpy as np
from snowflake.ml.modeling.metrics import r2_score
# misc
import joblib
import cachetools

import hextoolkit
hex_snowflake_conn = hextoolkit.get_data_connection('MY_SNOWFLAKE')
session = hex_snowflake_conn.get_snowpark_session()

DAILY_VISITORS_DF=session.table('DAILY_VISITORS') 
DAILY_VISITORS_DF.show()

# Since our dataset is a timeseries, we will split it based on a cut-off date (not a random split) to preserve the order and structure.  
split_date = '01-Sep-2022'
# Create Train DF 
train_df = DAILY_VISITORS_DF\
    .select('DAY',\
            'CALENDAR_MTH_DAY_NBR',\
            'CALENDAR_MTH',\
            'CALENDAR_YEAR',\
            'HOLIDAY',\
            'LAST_YEAR_DAILY_VISITORS',\
            'DAILY_VISITORS').\
    filter((F.col('CALENDAR_DATE') < split_date))
# Create Test DF Similar to Train_DF 
test_df = DAILY_VISITORS_DF\
    .select('DAY',\
            'CALENDAR_MTH_DAY_NBR',\
            'CALENDAR_MTH',\
            'CALENDAR_YEAR',\
            'HOLIDAY',\
            'LAST_YEAR_DAILY_VISITORS',\
            'DAILY_VISITORS').\
    filter((F.col('CALENDAR_DATE') >= split_date))
# Show train_df
train_df.show()

# Categorize all the features for modeling
CATEGORICAL_COLUMNS = ["DAY"]
CATEGORICAL_COLUMNS_OE = ["CALENDAR_WEEK_DAY_NBR"]
NUMERICAL_COLUMNS = ['CALENDAR_MTH_DAY_NBR','CALENDAR_MTH','CALENDAR_YEAR','HOLIDAY','LAST_YEAR_DAILY_VISITORS']
LABEL_COLUMNS = ['DAILY_VISITORS']
OUTPUT_COLUMNS = ['FORECASTED_DAILY_VISITORS']
# Create categories to be used in the OrdinalEncoder transformer. 
categories = {
    "DAY": np.array(["MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"]),
}
# Define a pipeline that does the preprocessing (OrdinalEncoder) for column DAY and Regressor (using XGBRegressor model)
pipe =  Pipeline(
    steps=[
        (
            "OE", OrdinalEncoder(
                input_cols=CATEGORICAL_COLUMNS,
                output_cols=CATEGORICAL_COLUMNS_OE,
                categories=categories,
                drop_input_cols=True
            )
        ),
            (
                "regressor", XGBRegressor(
                    input_cols=CATEGORICAL_COLUMNS_OE + NUMERICAL_COLUMNS,
                    label_cols=LABEL_COLUMNS,
                    output_cols=OUTPUT_COLUMNS,
                    n_jobs=-1
                )
            )
        
    ]
) 

# Train the model by calling .fit()
xgb_visitors_model=pipe.fit(train_df)

# Forecast daily visitors for test_df
result=xgb_visitors_model.predict(test_df)
# Show results
result.show()
# calculate Model Accuracy using R-2 Score 
print('Model Accuracy:' , r2_score(df=result,y_true_col_name='DAILY_VISITORS',y_pred_col_name='FORECASTED_DAILY_VISITORS'))

NOTE : No need for GridSearchCV for hyper-parameters tuning because of higher accuracy

# call pipe.fit(train_df) then convert Model into SKLEARN Object using to_sklearn()
daily_visitors_model=pipe.fit(train_df).to_sklearn()
# Save the model locally as joblib
MODEL_FILE = 'daily_visitors_model.joblib'
joblib.dump(daily_visitors_model,MODEL_FILE)
# Upload the model's joblib file into the Snowflake stage ML_FILES 
session.file.put(MODEL_FILE, "@ML_FILES" ,overwrite=True,auto_compress=False)
# Define a function to read the model from a file
@cachetools.cached(cache={})
def read_file(filename):
    import joblib
    import sys
    import os
    IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
    import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]
    if import_dir:
        with open(os.path.join(import_dir, filename), 'rb') as file:
            m = joblib.load(file)
            return m
# Create a Vectorized UDF for forecasting
@F.udf(name="daily_visitors_forecasting",
        is_permanent=True,
        stage_location='@ML_FILES',
        imports=['@ML_FILES/daily_visitors_model.joblib'],
        packages=['snowflake-ml-python','joblib','scikit-learn==1.2.2', 'xgboost==1.7.3', 'cachetools'],
        replace=True,
        session=session
)
def daily_visitors_forecasting(pd_input: T.PandasDataFrame[str, float, float, float, float, float]) -> T.PandasSeries[float]:
    features = ["DAY","CALENDAR_MTH_DAY_NBR","CALENDAR_MTH","CALENDAR_YEAR","HOLIDAY","LAST_YEAR_DAILY_VISITORS"]
    pd_input.columns = features
    model = read_file('daily_visitors_model.joblib')
    if model is not None:
        forecasting = model.predict(pd_input)
        return forecasting
    else:
        raise ValueError("The model is None. Check the model loading process.")

# Load DAILY_VISITORS_NEW Table into a Snowpark DF
new_dates = session.table('DAILY_VISITORS_NEW')
# Apply the UDF on the Snowpark DF
daily_visitors_forecasting = new_dates.with_column("forecasted_daily_visitors", F.call_function("daily_visitors_forecasting",
F.col("DAY"), F.col("CALENDAR_MTH_DAY_NBR"), F.col("CALENDAR_MTH"), F.col("CALENDAR_YEAR"), F.col("HOLIDAY"),
F.col("LAST_YEAR_DAILY_VISITORS"))
)
# Show the result
daily_visitors_forecasting.show()

# Write forecasting to a Snowflake table
daily_visitors_forecasting.write.mode('overwrite').save_as_table('daily_visitors_new_forecasting')