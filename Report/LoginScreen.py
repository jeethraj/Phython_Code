import cx_Oracle
from config_report import username, password, host, port, service_name
import streamlit as st
import pandas as pd

# Connect to Oracle
oracle_dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
oracle_connection_string = f"{username}/{password}@{oracle_dsn}"
oracle_connection = cx_Oracle.connect(oracle_connection_string)
oracle_cursor = oracle_connection.cursor()

# Function to decrypt password
def decrypt_password(password):
    query = f"SELECT CB_DECRYPTION('{password}') FROM DUAL"
    oracle_cursor.execute(query)
    result = oracle_cursor.fetchone()
    print('Decr',result)
    if result:
        return result[0]
    return None

def get_password(username):
    query = f"SELECT PASSWORD_V FROM CB_USERS WHERE USER_NAME_V='{username}'"
    oracle_cursor.execute(query)
    result = oracle_cursor.fetchone()
    if result:
        password = result[0]  # Access the first element of the tuple
        print(password)
        return password
    return None
# Login page

def login():
    st.title("Login")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    if st.button("Login"):
        # Fetch password from database
        db_password = get_password(username)
        if db_password:
            # Decrypt entered password and compare with db_password
            decrypted_password = decrypt_password(db_password)  # Pass db_password to decrypt_password function
            if decrypted_password and decrypted_password == password:
                st.success("Login successful!")
                return True
            else:
                st.error("Invalid username or password")
        else:
            st.error("Invalid username or password")
    return False

