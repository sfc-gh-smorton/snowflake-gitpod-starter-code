import os
import streamlit as st
import snowflake.connector  #upm package(snowflake-connector-python==2.7.0)
import altair as alt
 
# Initialize connection, using st.experimental_singleton to only run once.
@st.experimental_singleton
def init_connection():
    con = snowflake.connector.connect(
        user=os.getenv("SFUSER"),
        password=os.getenv("PASSWORD"),
        account=os.getenv("ACCOUNT"),
        role=os.getenv("ROLE"),
        warehouse=os.getenv("WAREHOUSE"),
    )
    return con
 
 
# Perform query, using st.experimental_memo to only rerun when the query changes or after 10 min.
@st.experimental_memo(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetch_pandas_all()
 
 
# rows = run_query("SHOW TABLES;")
conn = init_connection()
 
st.header('Sales Intelligence App')


# get SFLK connection
conn = init_connection()

# run query 
query = "select parent_account_name, account_name, g.lat as \"lat\", g.lng as \"lon\",lead_score, case when lead_score >.70 then 'High' when lead_score <.50 then 'Low' else 'Medium' end as Lead_score_rank from streamlit_hackathon.hackathon_schema.geo_health_hack g left join streamlit_hackathon.hackathon_schema.account_x_walk x on g.parent_account_id=x.parent_account_id and g.account_id =x.account_id;"
query_2= "select parent_account_name, account_name,case when lead_score >.70 then 'High' when lead_score <.50 then 'Low' else 'Medium' end as Lead_score_rank from streamlit_hackathon.hackathon_schema.geo_health_hack g left join streamlit_hackathon.hackathon_schema.account_x_walk x on g.parent_account_id=x.parent_account_id and g.account_id =x.account_id;"
query_3 ="select account_name,  month (to_date(\"date\")) as \"Month\", sum(\"sales_gts\") as \"Total_Sales\" from streamlit_hackathon.hackathon_schema.product_sales_new psn left join streamlit_hackathon.hackathon_schema.account_x_walk x on psn.new_account_id =x.account_id group by 1, 2;"
query_4 ="select account_name, sum(\"sales_gts\") as \"Total_Sales\" from streamlit_hackathon.hackathon_schema.product_sales_new psn left join streamlit_hackathon.hackathon_schema.account_x_walk x on psn.new_account_id =x.account_id group by 1 order by 2 desc;"
#query_3 ="select account_name, \"brand_name\", month (to_date(\"date\")) as \"Month\", sum(\"units\") as \"Total_Units\", sum(\"sales_gts\") as \"Total_Sales\" from streamlit_hackathon.hackathon_schema.product_sales_new psn left join streamlit_hackathon.hackathon_schema.account_x_walk x on psn.new_account_id =x.account_id group by 1, 2,3;"

city_df2 =run_query(query_2)
city_df = run_query(query)
phys_df = run_query(query_3)
phys_df2 = run_query(query_4)
# update query with a filter anytime the rank_filter component is updated


# display map


tab1, tab2 = st.tabs(["Sales Intelligence Score", "Physicians Sales"])

with tab1:
   st.subheader("Sales Territory")
   rank_filter = st.multiselect('Ranking', ["High","Medium", "Low"], "High")
   city_df = city_df.query('LEAD_SCORE_RANK in @rank_filter')
   st.map(city_df)
   st.dataframe(city_df)


with tab2:
   st.subheader("Sales by Physician")
   c=alt.Chart(phys_df).mark_bar().encode(
    x='Month',
    y='Total_Sales',
    color='ACCOUNT_NAME'
    )
   st.altair_chart(c, use_container_width=True)
   st.dataframe(phys_df2)

 


# Print results.
#for row in rows:
#    st.write(row)
