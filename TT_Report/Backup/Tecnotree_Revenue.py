import streamlit as st
import pandas as pd
import cx_Oracle
from streamlit_option_menu import option_menu
from config_report import username, password, host, port, service_name

def establish_oracle_connection():
    oracle_dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
    oracle_connection_string = f"{username}/{password}@{oracle_dsn}"
    oracle_connection = cx_Oracle.connect(oracle_connection_string)
    oracle_cursor = oracle_connection.cursor()
    return oracle_connection, oracle_cursor

def home_tab(oracle_cursor):
    st.title("Customer Details and Revenue")
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
    # Add your migrated query code here...

def pay_as_you_go_query(oracle_cursor):
    start_date_col, end_date_col, offer_type = st.columns(3)
    # Add your pay as you go query code here...

def revenue_tab():
    st.title("Customer Revenu Deatils")

    selected_tab = option_menu(
        menu_title=None,
        options=["Customer Revenue", "Migrated Customer", "Payas You Go"],
        icons=["house", "cash", "envelope"],
        default_index=0,
		orientation= "horizontal",
    )
    if selected_tab =="Customer Revenue":
        oracle_connection, oracle_cursor = establish_oracle_connection()
        offer_revenue_query(oracle_cursor)
        if oracle_cursor:
            oracle_cursor.close()
        if oracle_connection:
            oracle_connection.close()

    if selected_tab == "Migrated Customer":
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
    st.title("Logout")
    # Add logout-related content here

st.set_page_config(page_title="Report - Tecnotree")

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
elif selected == "Revenue":
    revenue_tab()
elif selected == "Contact":
    contact_tab()
elif selected == "Logout":
    logout_tab()

if oracle_cursor:
    oracle_cursor.close()
if oracle_connection:
    oracle_connection.close()
