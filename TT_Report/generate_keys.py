import pickle
from pathlib import Path

import streamlit_authenticator as stauth

names = ["Gunasekar","Jeethraj"]
usernames = ["Guna","Jeeth","Sheetal"]
passwords = ["TTReport#2024","jeeth#2024","sheetal#2024"]


hashed_passwords = stauth.Hasher(passwords).generate()

file_path = Path(__file__).parent / "hashed_pw.pkl"
with file_path.open("wb") as file:
    pickle.dump(hashed_passwords,file)