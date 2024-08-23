import pickle
from pathlib import Path
import streamlit_authenticator as stauth

import streamlit as st
import pandas as pd
import cx_Oracle
from streamlit_option_menu import option_menu
from config_report import username, password, host, port, service_name

st.set_page_config(page_title="Report - Tecnotree")

# User authentication details
names = ["Gunasekar", "Jeethraj"]
usernames = ["Guna", "Jeeth"]

# Load hashed passwords
file_path = Path(__file__).parent / "hashed_pw.pkl"
with file_path.open("rb") as file:
    hashed_passwords = pickle.load(file)

# Initialize authenticator
authenticator = stauth.Authenticate(names, usernames, hashed_passwords ,"Home", "abcdef")    

# Perform user login
name, authentication_status, usernames = authenticator.login("Login", "sidebar")


if authentication_status == False:
    st.error("Username/Password is incorrect")

if authentication_status == None:
    st.warning("Please enter your username and Password")

if authentication_status:    

    def establish_oracle_connection():
        oracle_dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
        oracle_connection_string = f"{username}/{password}@{oracle_dsn}"
        oracle_connection = cx_Oracle.connect(oracle_connection_string)
        oracle_cursor = oracle_connection.cursor()
        return oracle_connection, oracle_cursor

    def home_tab(oracle_cursor):
        st.title("Onboarding and Migrated Customer Details")
        start_date_col, end_date_col = st.columns(2)

        with start_date_col:
            start_date_Cus = st.date_input("Start Date", pd.to_datetime("today") - pd.to_timedelta(3, unit="D"),
                                        key='start_date_cus')

        with end_date_col:
            end_date_cus = st.date_input("End Date", pd.to_datetime("today"), key='end_date_cus')

        total_onboarding_query = """
        SELECT COUNT(1) FROM GSM_SERVICE_MAST 
        WHERE TRUNC(ACTIVATION_DATE_D)  BETWEEN TO_DATE(:start_date_Cus, 'YYYYMMDD') AND TO_DATE(:end_date_cus, 'YYYYMMDD')
        AND STATUS_CODE_V = 'AC' 
        AND CONTRACT_TYPE_V = 'P' 
        AND ACTIVATED_BY_USER_CODE_N != 226561
        """

        oracle_cursor.execute(total_onboarding_query,
                            start_date_Cus=start_date_Cus.strftime('%Y%m%d'),
                            end_date_cus=end_date_cus.strftime('%Y%m%d'))
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

    def offer_revenue_query(oracle_cursor):
        start_date_col, end_date_col = st.columns(2)

        with start_date_col:
            start_date_offer = st.date_input("Start Date", pd.to_datetime("today") - pd.to_timedelta(3, unit="D"), key='start_date_offer')

        with end_date_col:
            end_date_offer = st.date_input("End Date", pd.to_datetime("today"), key='end_date_offer')

        OFFER_REVEN = """
        SELECT TRUNC(EVENT_DATE_TIME_DT) AS EVENT_DATE ,COUNT(EVENT_TYPE_V) AS TOTAL_EVENT ,ROUND(SUM(CHARGE_AMOUNT_N/10000)) AS CHARGE_AMOUNT
        FROM CB_PREPAID_UPLOAD_ALL_EDRS
        WHERE EVENT_TYPE_V = '6'
        AND TRUNC(EVENT_DATE_TIME_DT) BETWEEN TO_DATE(:start_date_offer, 'YYYYMMDD') AND TO_DATE(:end_date_offer, 'YYYYMMDD')
        GROUP BY EVENT_TYPE_V, TRUNC(EVENT_DATE_TIME_DT)
        ORDER BY EVENT_DATE DESC
        """

        oracle_cursor.execute(OFFER_REVEN, start_date_offer=start_date_offer.strftime('%Y%m%d'), end_date_offer=end_date_offer.strftime('%Y%m%d'))
        oracle_data_offer = oracle_cursor.fetchall()

        Header_offer = ["EVENT_DATE", "TOTAL_EVENT", "OFFER_REVENUE"]
        data_con_offer = pd.DataFrame(oracle_data_offer, columns=Header_offer)
        st.write(data_con_offer)

    def migrated_query(oracle_cursor):
        start_date_col, end_date_col, Batch = st.columns(3)

        with start_date_col:
            start_Mig_date = st.date_input("Start Date", pd.to_datetime("today") - pd.to_timedelta(3, unit="D"), key='start_Mig_date')

        with end_date_col:
            end_Mig_date = st.date_input("End Date", pd.to_datetime("today"), key='end_Mig_date')

        # Fetch batches and their dates from the database
        batch_query = """
        SELECT 
            BATCH,
            BATCH_DATE
        FROM (
            SELECT 
                'Batch ' || DENSE_RANK() OVER (ORDER BY TO_DATE(BATCH_DATE, 'DD-MM-YYYY')) AS BATCH,
                BATCH_DATE
            FROM (
                SELECT DISTINCT TO_CHAR(REG_FORM_RECEIVED_DATE_D, 'DD-MM-YYYY') AS BATCH_DATE
                FROM GSM_SERVICE_MAST 
                WHERE reg_form_received_date_d IS NOT NULL 
            )
        )
        ORDER BY TO_DATE(BATCH_DATE, 'DD-MM-YYYY') ASC
        """

        oracle_cursor.execute(batch_query)
        batch_data = oracle_cursor.fetchall()
        batches = ["ALL"] + [batch[0] for batch in batch_data]
        batch_dates = {batch[0]: batch[1] for batch in batch_data}

        with Batch:
            selected_batch = st.selectbox("Select Batch", batches, index=batches.index("ALL"))

        selected_date = batch_dates.get(selected_batch, "")

        min_account = 0
        max_account = 0

        if selected_batch == "ALL":
            min_max_query = f"""
            SELECT 
            MIN(ACCOUNT_LINK_CODE_N) AS Min_Account_Link_Code,
            MAX(ACCOUNT_LINK_CODE_N) AS Max_Account_Link_Code
            FROM GSM_SERVICE_MAST
            WHERE ACTIVATED_BY_USER_CODE_N='226561'
            AND ACCOUNT_LINK_CODE_N NOT IN(629834800)
            """

            oracle_cursor.execute(min_max_query)
            min_max_data = oracle_cursor.fetchall()
            if min_max_data:
                min_account, max_account = min_max_data[0]
        else:
            if selected_date:
                min_max_query = f"""
                SELECT 
                MIN(ACCOUNT_LINK_CODE_N) AS Min_Account_Link_Code,
                MAX(ACCOUNT_LINK_CODE_N) AS Max_Account_Link_Code
                FROM GSM_SERVICE_MAST
                WHERE TRUNC(REG_FORM_RECEIVED_DATE_D) = TO_DATE('{selected_date}', 'DD-MM-YYYY')
                AND ACCOUNT_LINK_CODE_N NOT IN(629834800)
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

    def pay_as_you_go_query(oracle_cursor):
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

    def revenue_tab():
        selected_tab = option_menu(
            menu_title=None,
            options=["Over All", "Migrated", "Payas You Go"],
            default_index=0,
            orientation= "horizontal",
            styles={
                    "container": {"padding": "0!important", "background-color": "#fafafa"},
                    "nav-link": {
                        "font-size": "10px",
                        "text-align": "left",
                        "margin": "0px",
                        "--hover-color": "#eee",
                    },
            }
        )
        if selected_tab =="Over All":
            oracle_connection, oracle_cursor = establish_oracle_connection()
            offer_revenue_query(oracle_cursor)
            if oracle_cursor:
                oracle_cursor.close()
            if oracle_connection:
                oracle_connection.close()

        if selected_tab == "Migrated":
            oracle_connection, oracle_cursor = establish_oracle_connection()
            migrated_query(oracle_cursor)
            if oracle_cursor:
                oracle_cursor.close()
            if oracle_connection:
                oracle_connection.close()

        if selected_tab == "Payas You Go":
            oracle_connection, oracle_cursor = establish_oracle_connection()
            pay_as_you_go_query(oracle_cursor)
            if oracle_cursor:
                oracle_cursor.close()
            if oracle_connection:
                oracle_connection.close()

    def contact_tab():
        st.title("Contact")
        # Add contact-related content here

    def logout_tab():
        st.success("You have been logged out.")
        st.rerun()


    with st.sidebar:
        selected = option_menu(
            menu_title=None,
            options=["Home", "Revenue", "Contact", "Logout"],
            icons=["house", "cash", "envelope", "box-arrow-in-right"],
            default_index=0,
        )

    oracle_connection, oracle_cursor = establish_oracle_connection()

    if selected == "Home":
        home_tab(oracle_cursor)
    if selected == "Revenue":
        revenue_tab()
    if selected == "Contact":
        contact_tab()
    if selected == "Logout":
        logout_tab()

    if oracle_cursor:
        oracle_cursor.close()
    if oracle_connection:
        oracle_connection.close()