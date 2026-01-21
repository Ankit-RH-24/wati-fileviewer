import streamlit as st
import pandas as pd
import libsql_experimental as libsql
import os
from datetime import datetime, timedelta

# --- 1. PAGE CONFIG ---
st.set_page_config(layout="wide", page_title="WATI Chat Manager")

# --- 2. CSS ---
st.markdown("""
<style>
    .stDataFrame { width: 100%; }
    .block-container { padding-top: 4rem; padding-bottom: 2rem; }
    .stButton button { font-weight: bold; border-radius: 8px; }
    .stChatMessage {
        background-color: #f8f9fa; 
        color: #262730 !important;
        border: 1px solid #e9ecef;
        border-radius: 12px;
        padding: 15px;
        margin-bottom: 10px;
    }
    .stChatMessage p, .stChatMessage div, .stChatMessage span {
        color: #262730 !important;
    }
</style>
""", unsafe_allow_html=True)

# --- 3. CONNECTION ---
@st.cache_resource
def get_conn():
    url = os.getenv("TURSO_DB_URL")
    token = os.getenv("TURSO_DB_TOKEN")
    if not url or not token:
        st.error("Missing Secrets: TURSO_DB_URL or TURSO_DB_TOKEN")
        st.stop()
    return libsql.connect(database=url, auth_token=token)

conn = get_conn()

# --- 4. DATA FUNCTIONS (Optimized) ---

def run_query(query, params=()):
    """
    Manual query execution to avoid Pandas/SQLAlchemy warnings
    """
    try:
        cursor = conn.execute(query, params)
        columns = [description[0] for description in cursor.description]
        data = cursor.fetchall()
        return pd.DataFrame(data, columns=columns)
    except Exception as e:
        st.error(f"Query Error: {e}")
        return pd.DataFrame()

def extract_number(filename):
    if "-" in filename:
        return filename.split("-")[0]
    return filename.replace(".txt", "")

def get_batch_users(offset, limit=100, search_term=None):
    # OPTIMIZATION: Default to last 90 days if no search term
    # This uses the 'idx_timestamp' index to be INSTANT.
    
    if search_term:
        # Search mode: Must scan for keyword
        query = """
            SELECT 
                filename, 
                COUNT(*) as count, 
                MAX(timestamp) as last_active,
                (SELECT message_body FROM messages m2 
                 WHERE m2.filename = messages.filename 
                 ORDER BY timestamp DESC LIMIT 1) as preview
            FROM messages
            WHERE filename IN (
                SELECT DISTINCT filename FROM messages 
                WHERE message_body LIKE ? OR filename LIKE ?
            )
            GROUP BY filename
            ORDER BY last_active DESC
            LIMIT ? OFFSET ?
        """
        params = (f"%{search_term}%", f"%{search_term}%", limit, offset)
    else:
        # Browse mode: Restrict to recent history for speed
        # '2020-01-01' is a fallback, but practically we want the index to work.
        query = """
            SELECT 
                filename, 
                COUNT(*) as count, 
                MAX(timestamp) as last_active,
                (SELECT message_body FROM messages m2 
                 WHERE m2.filename = messages.filename 
                 ORDER BY timestamp DESC LIMIT 1) as preview
            FROM messages 
            WHERE timestamp > date('now', '-90 days') 
            GROUP BY filename 
            ORDER BY last_active DESC
            LIMIT ? OFFSET ?
        """
        params = (limit, offset)
        
    df = run_query(query, params)
    
    if not df.empty:
        df['Phone'] = df['filename'].apply(extract_number)
        # Handle cases where Last Active might be None or format it
        df['Last Active'] = pd.to_datetime(df['last_active'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M')
        return df[['Phone', 'count', 'preview', 'Last Active', 'filename']] 
    return pd.DataFrame()

def get_full_chat_history(filename):
    query = """
        SELECT timestamp, message_body 
        FROM messages 
        WHERE filename = ? 
        ORDER BY timestamp ASC
    """
    return run_query(query, (filename,))

# --- 5. STATE ---
if 'view_mode' not in st.session_state: st.session_state.view_mode = "list" 
if 'page_number' not in st.session_state: st.session_state.page_number = 0
if 'selected_file' not in st.session_state: st.session_state.selected_file = None
if 'clean_phone' not in st.session_state: st.session_state.clean_phone = ""

# --- 6. APP LOGIC ---

if st.session_state.view_mode == "list":
    st.title("📂 WATI Chat Manager")
    
    col_search, col_prev, col_stat, col_next = st.columns([4, 1, 2, 1])
    with col_search:
        search_query = st.text_input("🔍 Search", placeholder="Phone or Keyword...", label_visibility="collapsed")
    
    if search_query and st.session_state.page_number != 0:
        st.session_state.page_number = 0
        
    BATCH_SIZE = 100
    current_offset = st.session_state.page_number * BATCH_SIZE

    with col_prev:
        if st.button("⬅️ Prev"):
            if st.session_state.page_number > 0:
                st.session_state.page_number -= 1
                st.rerun()
    with col_next:
        if st.button("Next ➡️"):
            st.session_state.page_number += 1
            st.rerun()
    with col_stat:
        st.markdown(f"<div style='text-align: center; padding-top: 10px; color: #666;'>Row {current_offset}+</div>", unsafe_allow_html=True)

    # Load Data
    batch_df = get_batch_users(current_offset, BATCH_SIZE, search_query)

    if batch_df.empty:
        st.info("No active users found in the last 90 days. Try searching specifically.")
    else:
        event = st.dataframe(
            batch_df[['Phone', 'count', 'preview', 'Last Active']],
            column_config={
                "Phone": st.column_config.TextColumn("Contact", width="medium"),
                "count": st.column_config.NumberColumn("Msgs", width="small"),
                "preview": st.column_config.TextColumn("Latest Message", width="large"),
                "Last Active": st.column_config.TextColumn("Last Active", width="small"),
            },
            use_container_width=True,
            hide_index=True,
            selection_mode="single-row",
            on_select="rerun",
            height=700
        )
        if len(event.selection.rows) > 0:
            index = event.selection.rows[0]
            st.session_state.selected_file = batch_df.iloc[index]['filename']
            st.session_state.clean_phone = batch_df.iloc[index]['Phone']
            st.session_state.view_mode = "chat"
            st.rerun()

elif st.session_state.view_mode == "chat":
    col_back, col_title = st.columns([1, 10])
    with col_back:
        if st.button("⬅️ Back"):
            st.session_state.view_mode = "list"
            st.session_state.selected_file = None
            st.rerun()
    with col_title:
        st.subheader(f"💬 Conversation with {st.session_state.clean_phone}")

    chat_df = get_full_chat_history(st.session_state.selected_file)
    st.markdown("---")
    
    with st.container():
        for _, row in chat_df.iterrows():
            with st.chat_message("assistant"):
                st.write(row['message_body'])
                st.caption(f"{row['timestamp']}")