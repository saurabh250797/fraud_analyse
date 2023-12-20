import streamlit as st
import pandas as pd

from pyspark.sql import SparkSession
import plotly.express as px

# Load your data using Spark
spark = SparkSession.builder.appName('FraudVisualization').getOrCreate()
fraud_data = spark.read.csv('fraud.csv', header=True, inferSchema=True)
st.set_option('deprecation.showPyplotGlobalUse', False)

from pyspark.sql.functions import monotonically_increasing_id

fraud_data = fraud_data.withColumn('index', monotonically_increasing_id())

fraud_data_new = fraud_data.withColumn('isFraud', fraud_data['isFraud'].cast('string'))
fraud_data_new = fraud_data_new.withColumn('isFlaggedFraud', fraud_data_new['isFlaggedFraud'].cast('string'))


# Streamlit app
st.title('Fraud Data Visualization')

st.text("This Data contains {a} number of rows".format(a=fraud_data_new.count()))

fraud_data_new.createOrReplaceTempView('fraud')

results_1 = spark.sql('''SELECT 
                        type,
                        ROUND(mean(amount),2) as Average_Amount,
                        ROUND(mean(oldbalanceOrg),2) as AvgoldbalanceOrg,
                        ROUND(mean(newbalanceOrig),2) as Avgnewbalanceorig,
                        ROUND(mean(oldbalanceDest),2) as AvgoldbalanceDest,
                        ROUND(mean(newbalanceDest),2) as AvgnewbalanceDest
                        
                    FROM fraud 
                    GROUP BY type 
                    ORDER BY Average_Amount''')

avg_amount_by_type = results_1.toPandas()

# Streamlit app
st.title('Bar Chart Visualization')

# Dropdowns for x and y axis selection
x_axis = 'type'
y_axis = st.selectbox('Select Y-axis column:', avg_amount_by_type.columns[1:])  # Exclude 'type' column for y-axis

# Filter data for selected x-axis and y-axis columns
selected_data = avg_amount_by_type[[x_axis, y_axis]]

# Plotting using Plotly
st.header(f'Bar Chart: {x_axis} vs {y_axis}')
fig = px.bar(selected_data, x=x_axis, y=y_axis, title=f'{x_axis} vs {y_axis}')
st.plotly_chart(fig)


results_2 = spark.sql('''SELECT 
                        isFraud,
                        ROUND(mean(amount),2) as Average_Amount,
                        ROUND(mean(oldbalanceOrg),2) as AvgoldbalanceOrg,
                        ROUND(mean(newbalanceOrig),2) as Avgnewbalanceorig,
                        ROUND(mean(oldbalanceDest),2) as AvgoldbalanceDest,
                        ROUND(mean(newbalanceDest),2) as AvgnewbalanceDest
                        
                    FROM fraud 
                    GROUP BY isFraud 
                    ''')

avg_amount_by_fraud = results_2.toPandas()

# Dropdowns for x and y axis selection
x_axis_1 = 'isFraud'
unique_identifier_y = 1  # Change this identifier to ensure uniqueness

y_axis_1 = st.selectbox(f'Select Y-axis column {unique_identifier_y}: ', avg_amount_by_fraud.columns[1:], key=f"select_y_{unique_identifier_y}")  # Exclude 'fraud' column for y-axis

# Filter data for selected x-axis and y-axis columns
selected_data_1 = avg_amount_by_fraud[[x_axis_1, y_axis_1]]

# Plotting using Plotly
st.header(f'Bar Chart: {x_axis_1} vs {y_axis_1}')
fig_1 = px.bar(selected_data_1, x=x_axis_1, y=y_axis_1, title=f'{x_axis_1} vs {y_axis_1}')
st.plotly_chart(fig_1)
