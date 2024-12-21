import streamlit as st
import pandas as pd
import plotly.express as px
import databento as db

# Streamlit App Title
st.title("Order Book Imbalance Simulation Tool")

# Sidebar - API Key Input
st.sidebar.header("API Configuration")
api_key = st.sidebar.text_input("Enter your Databento API Key", type="password")

# Sidebar - Data Parameters
st.sidebar.header("Data Parameters")
symbol = st.sidebar.text_input("Symbol (e.g., GC)", value="GC")
start_date = st.sidebar.date_input("Start Date")
end_date = st.sidebar.date_input("End Date")

data_uploaded = False

if api_key and symbol and start_date and end_date:
    # Fetch Historical Data using Databento API
    st.sidebar.header("Fetch Historical Data")
    fetch_data = st.sidebar.button("Fetch Data")

    if fetch_data:
        st.write("Fetching data from Databento...")
        
        try:
            # Initialize Databento API client
            client = db.Historical(api_key)  # Pass the API key directly
            
            # Fetch data using the timeseries API
            response = client.timeseries.get_range(
                dataset="GLBX.MDP3",
                symbols=[symbol],
                schema="trades",
                start=str(start_date),
                end=str(end_date)
            )

            # Convert to DataFrame
            data = pd.DataFrame(response)
            data_uploaded = True
            
            # Display fetched data
            st.write("Fetched Data:")
            st.dataframe(data.head())

            # Debug: Show available columns
            st.write("Columns in fetched data:", list(data.columns))

        except Exception as e:
            st.error(f"Error fetching data: {e}")

# If data is uploaded or fetched, process it
if data_uploaded:
    # Process Data
    st.header("Data Processing")
    
    # Check for 'ts_event' column
    if 'ts_event' in data.columns:
        data['ts_event'] = pd.to_datetime(data['ts_event'], unit='ns')
        data['date'] = data['ts_event'].dt.date
        eod_data = data.groupby('date').apply(lambda x: x.iloc[-1])

        # Calculate Imbalance
        eod_data['bid_volume'] = eod_data.apply(lambda x: x['size'] if x['side'] == 'Bid' else 0, axis=1)
        eod_data['ask_volume'] = eod_data.apply(lambda x: x['size'] if x['side'] == 'Ask' else 0, axis=1)
        eod_data['imbalance'] = (eod_data['bid_volume'] - eod_data['ask_volume']) / (eod_data['bid_volume'] + eod_data['ask_volume'])

        # Simulate Next-Day Price Impact (Assume we have next-day open prices in data)
        eod_data['price_change'] = eod_data['price'].pct_change()

        # Display Calculated Metrics
        st.write("End-of-Day Data with Imbalance:")
        st.dataframe(eod_data[['date', 'imbalance', 'price_change']])

        # Visualization
        st.header("Visualization")

        # Scatter Plot - Imbalance vs. Price Change
        st.subheader("Imbalance vs. Next-Day Price Change")
        fig = px.scatter(eod_data, x="imbalance", y="price_change", title="Imbalance vs Price Change",
                         labels={"imbalance": "Order Book Imbalance", "price_change": "Next-Day Price Change (%)"})
        st.plotly_chart(fig)

        # Correlation Analysis
        correlation = eod_data[['imbalance', 'price_change']].corr().iloc[0, 1]
        st.write(f"Correlation between Imbalance and Price Change: {correlation:.2f}")
    else:
        st.error("The 'ts_event' column is missing in the fetched data. Unable to process end-of-day snapshots.")

# Instructions
if not data_uploaded:
    st.info("Configure the API and fetch data to begin.")
