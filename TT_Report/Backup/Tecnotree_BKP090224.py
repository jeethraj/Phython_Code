import streamlit as st
from streamlit_option_menu import option_menu

st.set_page_config(page_title="Report - Tecnotree")
with st.sidebar:
    selected= option_menu (
        menu_title=None,
        options= ["Home","Revenue","Contact"],
        icons= ["house","cash","envelope"],
        default_index=0,
        )  

if selected == "Home":
    st.title(f"You have selected {selected}")

if selected == "New Customer":
    st.title(f"You have selected {selected}")

if selected == "Migrated Customer":
    st.title(f"You have selected {selected}")

if selected == "Payas you Go":
    st.title(f"You have selected {selected}")
