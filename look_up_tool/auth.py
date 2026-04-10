import streamlit as st

def check_password():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    # If already logged in, continue
    if st.session_state.authenticated:
        return True

    st.title("Enter Password")

    password = st.text_input("Password", type="password")

    if st.button("Enter"):
        if password == "P4@admin":
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("Wrong password")

    return False