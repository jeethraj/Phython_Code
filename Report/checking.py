import streamlit as st
import pandas as pd
import cx_Oracle

# Connect to Oracle (Assuming config_report.py contains your credentials)
from config_report import username, password, host, port, service_name

# Define connection string
oracle_dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
oracle_connection_string = f"{username}/{password}@{oracle_dsn}"
oracle_connection = cx_Oracle.connect(oracle_connection_string)
oracle_cursor = oracle_connection.cursor()

# Set page title and configuration
st.set_page_config(page_title="Report - Tecnotree", layout="wide")  # Adjust layout as needed

# Add custom CSS
st.markdown("""
    <style>
        /* Add your custom CSS styles here */
        .stApp {
            max-width: 1200px;
            margin: auto;
        }
        /* Example: Increase width and center align the app */
    </style>
""", unsafe_allow_html=True)

# Your existing code continues from here...