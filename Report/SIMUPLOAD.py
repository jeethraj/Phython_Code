import streamlit as st
import pandas as pd
import cx_Oracle
from config_report import username, password, host, port, service_name

def upload_file(file):
    if file.type == 'text/plain':
        df = pd.read_csv(file, sep='\t')  # Assuming tab-separated TXT file
    else:
        df = pd.read_csv(file)

    return df

def query_oracle(sim_nums):
    if sim_nums.any():
        sim_nums_str = ', '.join([f"'{sim}'" for sim in sim_nums])

        # Connect to Oracle Database
        oracle_dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
        oracle_connection_string = f"{username}/{password}@{oracle_dsn}"
        oracle_connection = cx_Oracle.connect(oracle_connection_string)
        oracle_cursor = oracle_connection.cursor()


        # Execute the query
        query = f"SELECT * FROM GSM_SIMS_MASTER WHERE SIM_NUM_V IN ({sim_nums_str})"
        oracle_cursor.execute(query)
        result = oracle_cursor.fetchall()

        # Close the connection
        oracle_cursor.close()
        oracle_connection.close()

        return result
    else:
        return []
    
st.title('Upload File and Query Oracle')

uploaded_file = st.file_uploader("Choose a file (CSV or TXT)", type=["csv", "txt"])

if uploaded_file is not None:
    df = upload_file(uploaded_file)

    st.write('### Data from File:')
    st.write(df)

    all_sim_nums = df['SIM_NUM_V'].unique()

    if st.button('Query Oracle'):
        result = query_oracle(all_sim_nums)

        st.write('### Result from Oracle:')
        st.write(result)