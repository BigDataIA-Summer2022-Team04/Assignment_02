import streamlit as st
import requests


def fetch(session, url):
    try:
        result = session.get(url)
        return result.status_code,result.json()
    except Exception:
        return {}

def main():
    st.set_page_config(page_title="Jui's API ", page_icon="🤖")
    st.title("APIs")
    session = requests.Session()
    #with st.form("my_form"):

    #Function1
    st.subheader("**Get_Popular_Engine_Count :hammer_and_wrench::gear::** ")

    submitted1 = st.button(label="Search",key=1)

    if submitted1:
        st.write("Response")
        response_code,data = fetch(session,"http://127.0.0.1:8000/get_popular_engine_count")
        st.write("Response Code: "+str(response_code))
        if data:
            
            st.write(data)
        else:
            st.error("Error")
    
    #Function2
    st.subheader("**Get_Company_Address :round_pushpin::**")
    N_NUMBER = st.text_input("N_NUMBER", key="index")
    submitted2 = st.button(label="Search",key=2)

    if submitted2:
        st.write("Response")
        response_code,data = fetch(session,"http://127.0.0.1:8000/get_company_address?N_NUMBER="+N_NUMBER)
        st.write("Response Code: "+str(response_code))
        if data:
            st.write(data)
        else:
            st.error("Error")
    
    #Function3
    st.subheader("**Flight_Details_Between_Years :small_airplane: :**")
    start_date = st.text_input("Start Date", key="index1")
    end_date = st.text_input("End Date", key="index2")

    submitted3 = st.button(label="Search",key=3)

    if submitted3:
        response_code,data  = fetch(session,"http://127.0.0.1:8000/flight_details_between_years?start_date={}&end_date={}".format(start_date,end_date))
        st.write("Response Code: "+str(response_code))
        if data:
            
            st.json(data)
        else:
            st.error("Error")
    

if __name__ == '__main__':
    main()