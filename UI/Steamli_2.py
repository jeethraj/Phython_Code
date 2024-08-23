import cx_Oracle
import streamlit as st
import pandas as pd
import oracledb as cx_Oracle 
from ConfData import username, password, host, port, service_name

# Apply custom CSS to change background color
# Apply custom CSS to change background color
custom_css = """
    <style>
    body {
        background-color: black;
        color: white;
    }
    </style>
"""
st.markdown(custom_css, unsafe_allow_html=True)

cx_Oracle.init_oracle_client(lib_dir="C:\oracle_instant_client")

dns = cx_Oracle.makedsn(host, port, service_name=service_name)
#print(f"{dns=}")
connectionString = f"{username}/{password}@{dns}"

 # Create a database connection
connection = cx_Oracle.connect(connectionString)
    #connection = cx_Oracle.connect(username , password, dns)
cursor = connection.cursor()

# Date input fields

col1, col2 = st.columns(2)  # Create two columns

with col1:
    start_date = st.date_input('Select Start Date', pd.to_datetime('today') - pd.to_timedelta(10, unit='D'))

with col2:
    end_date = st.date_input('Select End Date', pd.to_datetime('today'))



# Format the date variables for Oracle
start_date_oracle_format = start_date.strftime("%Y-%m-%d")
end_date_oracle_format = end_date.strftime("%Y-%m-%d")

OfferRevenue = f"""SELECT  TRUNC(EVENT_DATE_TIME_DT) AS DATED,ROUND(SUM(A.CHARGE_AMOUNT_N/10000),0) AS REVENUE FROM CB_PREPAID_UPLOAD_ALL_EDRS A
                    WHERE A.EVENT_TYPE_V = 6  AND TRUNC(EVENT_DATE_TIME_DT) BETWEEN TO_DATE('{start_date_oracle_format}', 'YYYY-MM-DD') AND TO_DATE('{end_date_oracle_format}', 'YYYY-MM-DD')
                    GROUP BY TRUNC(EVENT_DATE_TIME_DT) ORDER BY DATED DESC"""
cursor.execute(OfferRevenue)
OfferRevenue_result = cursor.fetchall()
QUERY5_HEADERS = ["DATE", "REVENUE"]
df1 = pd.DataFrame(OfferRevenue_result, columns=QUERY5_HEADERS)
# Close the database connection
cursor.close()
connection.close()

#print (df1)

#st.title('Offer Revenue Data')

# Use st.beta_expander to create an expandable container
with st.expander("Offer Revenue Data", expanded=True):
        st.write(df1)