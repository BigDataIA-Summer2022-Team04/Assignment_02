import streamlit as st
import requests
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

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
    st.markdown('This function gives the aggregate details of flights with graph based on the count of engine used in them. From which we can get the data of popular engines used.')
    
    submitted1 = st.button(label="Search",key=1)

    if submitted1:
        st.write("Response")
        response_code,data = fetch(session,"http://127.0.0.1:8000/get_popular_engine_count")
        st.write("Response Code: "+str(response_code))
        if str(response_code)=="200":
           

            st.write(data)
            da=pd.json_normalize(data)
            
            fig2=plt.figure()
            sns.barplot(x='NAME', y='COUNT_ENGINE_TYPE', data=da)
            plt.xticks(rotation=90)
            st.pyplot(fig2)
            

        # # Add histogram data
        # x1 = np.random.randn(100) - 2
        # x2 = np.random.randn(100)
        # x3 = np.random.randn(100) + 2


        # # Group data together
        # hist_data = [x1, x2, x3]

        # group_labels = ['Group 1', 'Group 2', 'Group 3']

        # # Create distplot with custom bin_size
        # fig = ff.create_distplot(
        #      hist_data, group_labels, bin_size=[.1, .25, .5])

        # # Plot!
        # st.plotly_chart(fig, use_container_width=True)


        else:
            st.error(data)

    
    #Function2
    st.subheader("**Get_Company_Address :round_pushpin::**")
    st.markdown('This function gives the complete address of the registered company based on the flight ID. Accepts N_Number as input and returns records of flight number, company name, street, street2, city, state, zip code, region, country.')
    N_NUMBER = st.text_input("N_NUMBER", key="index")
    submitted2 = st.button(label="Search",key=2)

    if submitted2:
        st.write("Response")
        response_code,data = fetch(session,"http://127.0.0.1:8000/get_company_address?N_NUMBER="+N_NUMBER)
        st.write("Response Code: "+str(response_code))
        if str(response_code)=="200":
            st.write(data)
        else:
            st.error(data)
    
    #Function3
    st.subheader("**Flight_Details_Between_Years :small_airplane::date: :**")
    st.markdown('This function accepts two date values as an input and return the details of flight for which engine were manufactured between those particular year.')
    start_date = st.text_input("Start Date", key="index1")
    end_date = st.text_input("End Date", key="index2")

    submitted3 = st.button(label="Search",key=3)

    if submitted3:
        response_code,data  = fetch(session,"http://127.0.0.1:8000/flight_details_between_years?start_date={}&end_date={}".format(start_date,end_date))
        st.write("Response Code: "+str(response_code))
        if str(response_code)=="200":
            
            st.json(data)
            da=pd.json_normalize(data)
            u=da.groupby(['YEAR_MFR'])['YEAR_MFR'].count().reset_index(name='Count_Of_Flights')
            #u.columns=["Year","Count"]
            u["YEAR_MFR"]=u["YEAR_MFR"].str.slice(0,4)

            fig2=plt.figure()
            sns.barplot(x='YEAR_MFR', y='Count_Of_Flights', data=u)
            plt.xticks(rotation=0)
            st.pyplot(fig2)
            
            # fig2=plt.figure()
            # sns.barplot(x='N_NUMBER', y='YEAR_MFR', data=da)
            # plt.xticks(rotation=90)
            # st.pyplot(fig2)
        else:
            st.error(data)

        # df1 = pd.DataFrame.groupby(['start_date{}','end_date{}']).size().unstack()
        # df1.columns = pd.DataFrame.columns.droplevel()
        # df1.plot(kind='barh')
    

if __name__ == '__main__':
    main()