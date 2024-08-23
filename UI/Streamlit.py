import streamlit as st
import subprocess
import threading

# Function to run your script
def run_script():
    try:
        # You may need to adjust the path to your script
        script_path = r"C:\Users\poojaje\OneDrive - Tecnotree\Documents\Jeeth\Python\BASIC_API\SendMail.py"
        subprocess.Popen(["python3", script_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except Exception as e:
        st.error(f"Error: {str(e)}")

# Status of each query
query_statuses = {
    "query1": "Not started",
    "query2": "Not started",
    "query3": "Not started"
}

# Create the Streamlit app
st.title("Query Status Checker")

# Create buttons to run the script and refresh the status
if st.button("Run Script"):
    # Start a thread to run the script (non-blocking)
    script_thread = threading.Thread(target=run_script)
    script_thread.start()

if st.button("Refresh Status"):
    # You can add logic here to check the status of each query
    # For simplicity, we assume all queries have finished running
    query_statuses["query1"] = "Completed"
    query_statuses["query2"] = "Completed"
    query_statuses["query3"] = "Completed"

# Display the status of each query
st.header("Status of Queries:")
st.write(f"Query 1: {query_statuses['query1']}")
st.write(f"Query 2: {query_statuses['query2']}")
st.write(f"Query 3: {query_statuses['query3']}")
