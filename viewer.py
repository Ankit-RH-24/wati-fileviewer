import streamlit as st
import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta, time

# --- 1. CONFIGURATION ---
st.set_page_config(layout="wide", page_title="WATI Chat Manager")

# MongoDB URI (Hardcoded for your convenience)
try:
    # Try to get password from Streamlit Secrets (Cloud)
    MONGO_URI = st.secrets["MONGO_URI"]
except:
    # Fallback for local testing if secrets aren't set up
    # (You can keep your hardcoded link here ONLY if testing locally, 
    # but remove it before pushing if possible)
    MONGO_URI = "mongodb+srv://googlemcp:lookism24@cluster.idt477b.mongodb.net/?appName=Cluster"

# --- 2. CONNECT TO MONGO ---
@st.cache_resource
def get_collection():
    try:
        client = MongoClient(MONGO_URI)
        client.admin.command('ping') # Test connection
        return client["wati_logs"]["messages"]
    except Exception as e:
        st.error(f"❌ Connection Error: {e}")
        st.stop()

collection = get_collection()

# --- 3. DATA FUNCTIONS ---

def get_filtered_query(search_term, hide_templates, start_date, end_date):
    """Builds the MongoDB query based on all filters."""
    query = {}
    
    # 1. Date Range Filter
    # Combine date with time (Start of day 00:00:00 to End of day 23:59:59)
    start_dt = datetime.combine(start_date, time.min)
    end_dt = datetime.combine(end_date, time.max)
    query["timestamp"] = {"$gte": start_dt, "$lte": end_dt}

    # 2. Hide Templates
    if hide_templates:
        query["sender"] = {"$ne": "Template"}
    
    # 3. Search Term
    if search_term:
        query["$or"] = [
            {"filename": {"$regex": search_term, "$options": "i"}},
            {"message_body": {"$regex": search_term, "$options": "i"}}
        ]
    return query

@st.cache_data(ttl=300)
def get_active_users(limit, search_term, hide_templates, start_date, end_date):
    query = get_filtered_query(search_term, hide_templates, start_date, end_date)

    # Aggregation: Find active users in this date range
    pipeline = [
        {"$match": query},
        {"$sort": {"timestamp": -1}},
        {"$group": {
            "_id": "$filename",
            "last_msg": {"$first": "$message_body"},
            "last_sender": {"$first": "$sender"},
            "last_active": {"$first": "$timestamp"},
            "msg_count": {"$sum": 1}
        }},
        {"$sort": {"last_active": -1}},
        {"$limit": limit}
    ]
    
    results = list(collection.aggregate(pipeline))
    
    if not results:
        return pd.DataFrame()
        
    df = pd.DataFrame(results)
    df.rename(columns={"_id": "filename", "last_msg": "preview"}, inplace=True)
    df['Phone'] = df['filename'].apply(lambda x: x.split('-')[0] if '-' in x else x.replace('.txt', ''))
    
    return df

def get_bulk_export_data(filenames, hide_templates, start_date, end_date):
    """Fetches ALL messages for the listed users within the date range."""
    start_dt = datetime.combine(start_date, time.min)
    end_dt = datetime.combine(end_date, time.max)
    
    query = {
        "filename": {"$in": filenames},
        "timestamp": {"$gte": start_dt, "$lte": end_dt}
    }
    
    if hide_templates:
        query["sender"] = {"$ne": "Template"}

    cursor = collection.find(query).sort([("filename", 1), ("timestamp", 1)])
    
    df = pd.DataFrame(list(cursor))
    if not df.empty:
        # Clean up columns for CSV
        df['Phone'] = df['filename'].apply(lambda x: x.split('-')[0] if '-' in x else x)
        return df[['Phone', 'timestamp', 'sender', 'message_body', 'status']]
    return df

def get_chat_history(filename, hide_templates=False):
    # For individual chat view, we usually show FULL history, 
    # but you can pass dates here too if you want the view to be filtered.
    query = {"filename": filename}
    if hide_templates:
        query["sender"] = {"$ne": "Template"}
        
    cursor = collection.find(query).sort("timestamp", 1)
    return pd.DataFrame(list(cursor))

# --- 4. APP UI ---

# Session State
if 'view_mode' not in st.session_state: st.session_state.view_mode = "list"
if 'selected_file' not in st.session_state: st.session_state.selected_file = None

# --- SIDEBAR FILTERS ---
with st.sidebar:
    st.header("⚙️ Filters")
    
    # Date Range Input
    today = datetime.today()
    last_month = today - timedelta(days=30)
    date_range = st.date_input(
        "📅 Date Range",
        (last_month, today),
        format="DD/MM/YYYY"
    )
    
    # Handle single date selection vs range
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        # Fallback if user picks only one day
        start_date = date_range[0] if isinstance(date_range, tuple) and date_range else today
        end_date = today

    hide_tmps = st.checkbox("Hide Templates / Bots", value=True)
    limit = st.slider("Max Users to Show", 50, 1000, 100)
    
    st.markdown("---")
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

# === VIEW 1: DASHBOARD ===
if st.session_state.view_mode == "list":
    st.title("🚀 WATI Manager")
    
    search = st.text_input("🔍 Search Phone or Keyword...", placeholder="Enter phone number...")
    
    with st.spinner("Fetching data..."):
        df = get_active_users(limit, search, hide_tmps, start_date, end_date)
    
    if df.empty:
        st.info(f"No active users found between {start_date} and {end_date}.")
    else:
        # --- BATCH EXPORT SECTION ---
        with st.expander("📥 Export Data (Batch)", expanded=False):
            st.write(f"Ready to export messages for **{len(df)} users** visible in this list.")
            st.caption(f"Date Filter: {start_date} to {end_date}")
            
            if st.button("Generate Batch CSV"):
                with st.spinner("Downloading all messages..."):
                    export_df = get_bulk_export_data(df['filename'].tolist(), hide_tmps, start_date, end_date)
                    
                    if not export_df.empty:
                        csv = export_df.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="📄 Click to Download CSV",
                            data=csv,
                            file_name=f"wati_export_{start_date}_{end_date}.csv",
                            mime="text/csv"
                        )
                    else:
                        st.warning("No messages found to export.")

        # --- USER LIST TABLE ---
        st.caption(f"Found {len(df)} users active in this period.")
        
        event = st.dataframe(
            df[['Phone', 'last_sender', 'preview', 'last_active']],
            column_config={
                "Phone": st.column_config.TextColumn("Contact", width="medium"),
                "last_sender": "Sender",
                "preview": st.column_config.TextColumn("Latest Message", width="large"),
                "last_active": st.column_config.DatetimeColumn("Last Active", format="D MMM, HH:mm"),
            },
            use_container_width=True,
            hide_index=True,
            selection_mode="single-row",
            on_select="rerun",
            height=600
        )
        
        if len(event.selection.rows) > 0:
            index = event.selection.rows[0]
            st.session_state.selected_file = df.iloc[index]['filename']
            st.session_state.clean_phone = df.iloc[index]['Phone']
            st.session_state.view_mode = "chat"
            st.rerun()

# === VIEW 2: CHAT DETAIL ===
elif st.session_state.view_mode == "chat":
    # Header & Nav
    col1, col2, col3 = st.columns([1, 6, 2])
    with col1:
        if st.button("⬅️ Back"):
            st.session_state.view_mode = "list"
            st.rerun()
    with col2:
        st.subheader(f"💬 {st.session_state.clean_phone}")
    
    # Fetch History (Full history usually preferred for context)
    chat_df = get_chat_history(st.session_state.selected_file, hide_templates=False)
    
    # Export Single Chat
    with col3:
        if not chat_df.empty:
            csv = chat_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Export Chat", csv, f"{st.session_state.clean_phone}.csv", "text/csv")
            
    st.markdown("---")
    
    # Filters for View Only
    show_all = st.checkbox("Show Hidden Templates", value=False)

    # Render Chat
    for _, row in chat_df.iterrows():
        sender = row.get('sender', 'System')
        body = row.get('message_body', '')
        ts = row.get('timestamp', '')
        
        is_bot = sender in ["Template", "System"]
        
        # Skip bots if unchecked
        if is_bot and not show_all:
            continue
            
        role = "assistant" if is_bot else "user"
        
        with st.chat_message(role):
            st.write(body)
            st.caption(f"**{sender}** • {ts}")