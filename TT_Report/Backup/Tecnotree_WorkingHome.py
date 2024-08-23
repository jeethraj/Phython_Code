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


def revenue_tab():
    st.title("Revenue")
    # Add revenue-related content here


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

# Content based on selected option
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
