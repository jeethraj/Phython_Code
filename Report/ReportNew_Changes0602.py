import streamlit as st
import pandas as pd
import cx_Oracle
from config_report import username, password, host, port, service_name

# Connect to Oracle
oracle_dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
oracle_connection_string = f"{username}/{password}@{oracle_dsn}"
oracle_connection = cx_Oracle.connect(oracle_connection_string)
oracle_cursor = oracle_connection.cursor()

st.title("Customer Details and Revenue")


tabs = st.tabs(["HOME", "NEW CUSTOMER", "MIGRATED CUSTOMER","PAYAS YOU GO"])

# Home Tab
with tabs[0]:
    start_date_col, end_date_col = st.columns(2)

    with start_date_col:
        start_date_Cus = st.date_input("Start Date", pd.to_datetime("today") - pd.to_timedelta(3, unit="D"), key='start_date_cus')

    with end_date_col:
        end_date_cus = st.date_input("End Date", pd.to_datetime("today"), key='end_date_cus')
    
    total_onboarding_query = """
    SELECT COUNT(1) FROM GSM_SERVICE_MAST 
    WHERE TRUNC(ACTIVATION_DATE_D)  BETWEEN TO_DATE(:start_date_Cus, 'YYYYMMDD') AND TO_DATE(:end_date_cus, 'YYYYMMDD')
    AND STATUS_CODE_V = 'AC' 
    AND CONTRACT_TYPE_V = 'P' 
    AND ACTIVATED_BY_USER_CODE_N != 226561
    """

    
    oracle_cursor.execute(total_onboarding_query, start_date_cus=start_date_Cus.strftime('%Y%m%d'), end_date_cus=end_date_cus.strftime('%Y%m%d'))
    total_onboarding = oracle_cursor.fetchone()[0]

    st.write(f"Total Onboarding: {total_onboarding}")

    
    total_migrated_query = """
    SELECT COUNT(1) FROM GSM_SERVICE_MAST 
    WHERE STATUS_CODE_V = 'AC' 
    AND CONTRACT_TYPE_V = 'P' 
    AND ACTIVATED_BY_USER_CODE_N = 226561
    """

    
    oracle_cursor.execute(total_migrated_query)
    total_migrated = oracle_cursor.fetchone()[0]

    st.write(f"Total Migrated Number: {total_migrated}")

# OFFER_REVEN Tab
with tabs[1]:
    
    start_date_col, end_date_col = st.columns(2)

    with start_date_col:
        start_date_offer = st.date_input("Start Date", pd.to_datetime("today") - pd.to_timedelta(3, unit="D"), key='start_date_offer')

    with end_date_col:
        end_date_offer = st.date_input("End Date", pd.to_datetime("today"), key='end_date_offer')

    # Fetch data from Oracle - OFFER_REVEN
    OFFER_REVEN = """
          SELECT TRUNC(EVENT_DATE_TIME_DT) AS EVENT_DATE ,COUNT(EVENT_TYPE_V) AS TOTAL_EVENT ,ROUND(SUM(CHARGE_AMOUNT_N/10000)) AS CHARGE_AMOUNT
          FROM CB_PREPAID_UPLOAD_ALL_EDRS
          WHERE EVENT_TYPE_V = '6'
          AND TRUNC(EVENT_DATE_TIME_DT) BETWEEN TO_DATE(:start_date_offer, 'YYYYMMDD') AND TO_DATE(:end_date_offer, 'YYYYMMDD')
          GROUP BY EVENT_TYPE_V, TRUNC(EVENT_DATE_TIME_DT)
          ORDER BY EVENT_DATE DESC
          """

    # Execute query with parameters - OFFER_REVEN
    oracle_cursor.execute(OFFER_REVEN, start_date_offer=start_date_offer.strftime('%Y%m%d'), end_date_offer=end_date_offer.strftime('%Y%m%d'))
    oracle_data_offer = oracle_cursor.fetchall()

    Header_offer = ["EVENT_DATE", "TOTAL_EVENT", "OFFER_REVENUE"]

    data_con_offer = pd.DataFrame(oracle_data_offer, columns=Header_offer)
    st.write(data_con_offer)

# MIGRATED_REVEN Tab
with tabs[2]:
    
    start_date_col, end_date_col ,Batch = st.columns(3)

    with start_date_col:
        start_Mig_date = st.date_input("Start Date", pd.to_datetime("today") - pd.to_timedelta(3, unit="D"), key='start_Mig_date')

    with end_date_col:
        end_Mig_date = st.date_input("End Date", pd.to_datetime("today"), key='end_Mig_date')

    with Batch:
        selected_batch = st.selectbox("Select Batch", ["ALL","Batch 1", "Batch 2", "Batch 3", "Batch 4", "Batch 5"])

    # Define a dictionary to map batches to their corresponding date values
    batch_dates = {
        "Batch 1": "30-11-2023",
        "Batch 2": "12-12-2023",
        "Batch 3": "20-12-2023",
        "Batch 4": "10-01-2024",
        "Batch 5": "17-01-2024",
        "ALL" : ""
    }
    # Get the selected date for the batch
    selected_date = batch_dates[selected_batch]

    if selected_batch == "ALL":
        min_max_query = f"""
        SELECT 
        MIN(ACCOUNT_LINK_CODE_N) AS Min_Account_Link_Code,
        MAX(ACCOUNT_LINK_CODE_N) AS Max_Account_Link_Code
        FROM GSM_SERVICE_MAST
        WHERE ACTIVATED_BY_USER_CODE_N='226561'
        """
    else:
        selected_date = batch_dates.get(selected_batch, "")
        min_max_query = f"""
        SELECT 
        MIN(ACCOUNT_LINK_CODE_N) AS Min_Account_Link_Code,
        MAX(ACCOUNT_LINK_CODE_N) AS Max_Account_Link_Code
        FROM GSM_SERVICE_MAST
        WHERE TRUNC(REG_FORM_RECEIVED_DATE_D) = TO_DATE('{selected_date}', 'DD-MM-YYYY')
        """
    oracle_cursor.execute(min_max_query)
    min_max_data = oracle_cursor.fetchall()
    min_account, max_account = min_max_data[0]  
    
    MIGRATED_REVEN = f"""
    SELECT TRUNC(A.EVENT_DATE_TIME_DT) AS EVENT_DATE, COUNT(A.EVENT_TYPE_V) AS TOTAL_EVENT, ROUND(SUM(A.CHARGE_AMOUNT_N/10000)) AS CHARGE_AMOUNT 
    FROM CB_PREPAID_UPLOAD_ALL_EDRS A , CB_OFFERS B
    WHERE A.EVENT_TYPE_V = '6'
    AND  A.ATTRIBUTE1_V=B.APPLY_TARIFF_CODE_V
    AND A.INV_ACCNT_OR_SERV_ACCNT_CODE_N BETWEEN {min_account} AND {max_account}
    AND TRUNC(A.EVENT_DATE_TIME_DT) BETWEEN TO_DATE(:start_Mig_date, 'YYYYMMDD') AND TO_DATE(:end_Mig_date, 'YYYYMMDD')
    GROUP BY A.EVENT_TYPE_V, TRUNC(A.EVENT_DATE_TIME_DT)
    ORDER BY EVENT_DATE DESC
    """
    oracle_cursor.execute(MIGRATED_REVEN, start_Mig_date=start_Mig_date.strftime('%Y%m%d'), end_Mig_date=end_Mig_date.strftime('%Y%m%d'))
    oracle_data_migrated = oracle_cursor.fetchall()
    Header_migrated = ["EVENT_DATE", "TOTAL_EVENT", "MIG_REVENUE"]
    data_con_migrated = pd.DataFrame(oracle_data_migrated, columns=Header_migrated)
    st.write(data_con_migrated)

with tabs[3]:
    start_date_col, end_date_col, offer_type = st.columns(3)

    with start_date_col:
        start_date_pay = st.date_input("Start Date", pd.to_datetime("today") - pd.to_timedelta(3, unit="D"),key='start_date_pay')

    with end_date_col:
        end_date_pay = st.date_input("End Date", pd.to_datetime("today"), key='end_date_pay')

    with offer_type:
        selected_type = st.selectbox("Call Type", ["ALL","VOICE", "DATA", "SMS"])

    
    selected_call_type = {"VOICE": "001", "SMS": "031", "DATA": "18","ALL": ""}
    selected_call = selected_call_type[selected_type]

    if selected_type == "ALL":
        PAY_REVEN = """
            SELECT TRUNC(CALL_DATE_TIME_DT) AS EVENT_DATE, COUNT(CALL_TYPE_V) AS TOTAL_EVENT,
                   ROUND(SUM(CHARGE_AMOUNT_N/10000)) AS CHARGE_AMOUNT
            FROM CB_PREPAID_UPLOAD_ALL_CDRS
            WHERE ATTRIBUTE1_V='1'
            AND TRUNC(CALL_DATE_TIME_DT) BETWEEN TO_DATE(:start_pay_date, 'YYYYMMDD') AND TO_DATE(:end_pay_date, 'YYYYMMDD')
            GROUP BY TRUNC(CALL_DATE_TIME_DT)
            ORDER BY EVENT_DATE DESC
        """
    else:
        selected_call_type_value = selected_call_type[selected_type]

        PAY_REVEN = f"""
            SELECT TRUNC(CALL_DATE_TIME_DT) AS EVENT_DATE, COUNT(CALL_TYPE_V) AS TOTAL_EVENT,
                   ROUND(SUM(CHARGE_AMOUNT_N/10000)) AS CHARGE_AMOUNT
            FROM CB_PREPAID_UPLOAD_ALL_CDRS
            WHERE CALL_TYPE_V = :selected_call_type
            AND ATTRIBUTE1_V='1'
            AND TRUNC(CALL_DATE_TIME_DT) BETWEEN TO_DATE(:start_pay_date, 'YYYYMMDD') AND TO_DATE(:end_pay_date, 'YYYYMMDD')
            GROUP BY TRUNC(CALL_DATE_TIME_DT)
            ORDER BY EVENT_DATE DESC
        """

    if selected_type == "ALL":
        oracle_cursor.execute(PAY_REVEN, start_pay_date=start_date_pay.strftime('%Y%m%d'),end_pay_date=end_date_pay.strftime('%Y%m%d'))
    else:
        oracle_cursor.execute(PAY_REVEN, start_pay_date=start_date_pay.strftime('%Y%m%d'),end_pay_date=end_date_pay.strftime('%Y%m%d'),selected_call_type=selected_call_type_value)
    oracle_data_pay = oracle_cursor.fetchall()
    Header_offer = ["EVENT_DATE", "TOTAL_EVENT", "PAY_REVENUE"]
    data_con_pay = pd.DataFrame(oracle_data_pay, columns=Header_offer)
    st.write(data_con_pay)

oracle_cursor.close()
oracle_connection.close()