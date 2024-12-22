import streamlit as st
import pandas as pd
import plotly.express as px
import databento as db
from datetime import datetime, timedelta, date

# -----------------------------------------------------------------------------
# Helper Function: Split a date range into smaller (start, end) chunks
# -----------------------------------------------------------------------------
def chunk_date_range(start_date, end_date, chunk_size_days=7):
    """
    Splits the date range [start_date, end_date] into (start, end) tuples,
    each covering up to chunk_size_days.
    """
    chunks = []
    current_start = start_date
    while current_start < end_date:
        next_end = current_start + timedelta(days=chunk_size_days)
        if next_end > end_date:
            next_end = end_date
        chunks.append((current_start, next_end))
        current_start = next_end + timedelta(days=1)
    return chunks

# -----------------------------------------------------------------------------
# Per-Day Aggregator for Bid/Ask Volumes
# -----------------------------------------------------------------------------
class DailyAggregator:
    """
    Stores rolling bid_volume, ask_volume, last_price for each date.
    Example usage:
        aggregator = DailyAggregator()
        aggregator.update(dateobj, side, size, action, price)
        final_df = aggregator.to_dataframe()
    """
    def __init__(self):
        # { date_obj: { 'bid_volume': float, 'ask_volume': float, 'price': float } }
        self.daily_data = {}

    def _ensure_date(self, dateobj):
        if dateobj not in self.daily_data:
            self.daily_data[dateobj] = {
                "bid_volume": 0.0,
                "ask_volume": 0.0,
                "price": None  # last observed price
            }

    def update(self, dateobj, side, size, action, price):
        """
        Update volumes & price based on an event record.
        side: 'Bid' or 'Ask'
        action: Add, Cancel, Modify, Trade, Fill, clearBook, etc.
        size: numeric
        price: numeric (the most recent price)
        """
        self._ensure_date(dateobj)
        day_info = self.daily_data[dateobj]

        # Always update last price if we have a valid price
        if price is not None:
            day_info["price"] = float(price)

        side = side.lower() if side else "none"
        action = action.lower() if action else "none"
        size = float(size or 0.0)

        if action == "add":
            if side == "bid":
                day_info["bid_volume"] += size
            elif side == "ask":
                day_info["ask_volume"] += size

        elif action in ("cancel", "trade", "fill"):
            # Decrease volume from the side
            if side == "bid":
                day_info["bid_volume"] = max(0.0, day_info["bid_volume"] - size)
            elif side == "ask":
                day_info["ask_volume"] = max(0.0, day_info["ask_volume"] - size)

        elif action == "modify":
            # Naive approach: treat as "add" of new size. Real logic needs old size or IDs.
            if side == "bid":
                day_info["bid_volume"] += size
            elif side == "ask":
                day_info["ask_volume"] += size

        elif action == "clearbook":
            day_info["bid_volume"] = 0.0
            day_info["ask_volume"] = 0.0
        # action == "none" => do nothing

    def to_dataframe(self):
        """
        Returns a DataFrame with columns: date, bid_volume, ask_volume, price
        """
        rows = []
        for day, info in self.daily_data.items():
            rows.append({
                "date": day,
                "bid_volume": info["bid_volume"],
                "ask_volume": info["ask_volume"],
                "price": info["price"],
            })
        df = pd.DataFrame(rows)
        df.sort_values("date", inplace=True)
        return df

# -----------------------------------------------------------------------------
# Streamlit App
# -----------------------------------------------------------------------------
st.title("Optimized Event‐Aware Imbalance Tool (with DBN encoding)")

# Sidebar inputs
st.sidebar.header("API Configuration")
api_key = st.sidebar.text_input("Enter Databento API Key", type="password")

st.sidebar.header("Symbol & Date Range")
symbol_input = st.sidebar.text_input("Enter Symbol (e.g., GCG5)", value="GCG5")
start_date = st.sidebar.date_input("Start Date", value=date(2025, 1, 1))
end_date = st.sidebar.date_input("End Date", value=date(2025, 1, 15))

if start_date >= end_date:
    st.warning("Start date >= end date. Adjusting.")
    end_date = start_date + timedelta(days=1)

chunk_size_days = st.sidebar.number_input(
    "Chunk size (days)",
    min_value=1,
    max_value=60,
    value=7,
    help="Split date range into chunks to reduce memory usage."
)

st.sidebar.header("CSV Upload (Optional)")
uploaded_file = st.sidebar.file_uploader("Upload CSV File", type=["csv"])

fetch_data = st.sidebar.button("Fetch Data from Databento")

data_uploaded = False
final_df = None  # Will store final daily EOD data

# -----------------------------------------------------------------------------
# 1) If CSV is uploaded, parse it
# -----------------------------------------------------------------------------
if uploaded_file:
    try:
        input_data = pd.read_csv(uploaded_file)
        data_uploaded = True
        st.write("CSV Data Loaded (raw):")
        st.dataframe(input_data.head())
    except Exception as e:
        st.error(f"Error reading CSV file: {e}")

    # Process CSV data in aggregator if we have 'ts_event'
    if data_uploaded:
        if "ts_event" in input_data.columns:
            input_data["ts_event"] = pd.to_datetime(input_data["ts_event"], errors="coerce")
            input_data["date"] = input_data["ts_event"].dt.date
            input_data.sort_values("ts_event", inplace=True)
        else:
            st.error("CSV missing 'ts_event' column, can't process events.")

        aggregator = DailyAggregator()
        for idx, row in input_data.iterrows():
            d = row.get("date")
            side = row.get("side", "None")
            size = row.get("size", 0)
            action = row.get("action", "None")
            price = row.get("price", None)
            aggregator.update(d, side, size, action, price)

        final_df = aggregator.to_dataframe()
        # Compute imbalance + price_change
        final_df["imbalance"] = (
            (final_df["bid_volume"] - final_df["ask_volume"]) /
            (final_df["bid_volume"] + final_df["ask_volume"]).replace(0, pd.NA)
        )
        final_df["price_change"] = final_df["price"].astype(float).pct_change()
        st.write("Daily Aggregated Data from CSV:")
        st.dataframe(final_df)

# -----------------------------------------------------------------------------
# 2) Otherwise, fetch from Databento in chunks (with DBN encoding)
# -----------------------------------------------------------------------------
if fetch_data and api_key and not data_uploaded:
    try:
        st.write("Fetching data from Databento with DBN encoding in chunked mode...")

        # Attempt symbology resolution (optional)
        client = db.Historical(api_key)
        resolved_symbol = symbol_input
        try:
            res = client.symbology.resolve(
                dataset="GLBX.MDP3",
                symbols=[symbol_input],
                stype_in="raw_symbol",
                stype_out="instrument_id",
                start_date=str(start_date)
            )
            if len(res) > 0:
                resolved_symbol = res[symbol_input]
                st.write(f"Resolved symbol: {symbol_input} => {resolved_symbol}")
            else:
                st.warning("Could not resolve symbol. Using raw symbol directly.")
        except Exception as e_sym:
            st.warning(f"Symbology resolution failed, using raw symbol. Error: {e_sym}")

        # Create aggregator
        aggregator = DailyAggregator()

        date_chunks = chunk_date_range(start_date, end_date, chunk_size_days)
        chunk_progress = st.progress(0)
        total_chunks = len(date_chunks)

        for i, (c_start, c_end) in enumerate(date_chunks):
            st.write(f"Chunk {i+1}/{total_chunks}: {c_start} -> {c_end}")

            # IMPORTANT: Here we specify encoding="dbn" to speed up retrieval
            chunk_iter = client.timeseries.get_range(
                dataset="GLBX.MDP3",
                symbols=[resolved_symbol],
                schema="mbo",
                start=str(c_start),
                end=str(c_end),
                encoding="dbn"  # only works on older Databento clients (<1.0)
            )
            chunk_list = list(chunk_iter)
            st.write(f"  -> Fetched {len(chunk_list)} records in this chunk.")

            # Process each record on the fly
            for rec in chunk_list:
                action = getattr(rec, "action", None)
                side = getattr(rec, "side", None)
                size = getattr(rec, "size", 0)
                price = getattr(rec, "price", None)
                ts_event = getattr(rec, "ts_event", None)

                if ts_event is not None:
                    dt = pd.to_datetime(ts_event, unit="ns", errors="coerce")
                    d = dt.date()
                else:
                    d = None

                if d is not None:
                    aggregator.update(d, side, size, action, price)

            # Discard chunk_list
            chunk_list = None

            # Update progress
            chunk_progress.progress((i + 1) / total_chunks)

        # Once all chunks are processed, build final daily data
        final_df = aggregator.to_dataframe()
        data_uploaded = True
        st.write("All chunks processed. Daily Aggregated Data:")

        # Compute imbalance + price_change
        final_df["imbalance"] = (
            (final_df["bid_volume"] - final_df["ask_volume"]) /
            (final_df["bid_volume"] + final_df["ask_volume"]).replace(0, pd.NA)
        )
        final_df["price_change"] = final_df["price"].astype(float).pct_change()
        st.dataframe(final_df)

    except Exception as e:
        st.error(f"Error fetching data: {e}")

# -----------------------------------------------------------------------------
# 3) Visualization & Correlation
# -----------------------------------------------------------------------------
if final_df is not None and data_uploaded:
    st.header("Visualization")
    fig = px.scatter(
        final_df,
        x="imbalance",
        y="price_change",
        title="Imbalance vs. Next-Day Price Change",
        labels={"imbalance": "Order Book Imbalance", "price_change": "Next‐Day % Change"}
    )
    st.plotly_chart(fig)

    valid_rows = final_df.dropna(subset=["imbalance", "price_change"])
    if len(valid_rows) > 1:
        corr_val = valid_rows[["imbalance", "price_change"]].corr().iloc[0, 1]
        st.write(f"Correlation (Imbalance vs. Price Change): {corr_val:.2f}")
    else:
        st.write("Insufficient data for correlation.")

if not data_uploaded:
    st.info("Enter API key, symbol, date range, chunk size, then click 'Fetch Data' or upload a CSV file.")
