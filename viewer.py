import streamlit as st
import pandas as pd
import os
from datetime import datetime, timedelta

# --- 1. PAGE CONFIG ---
st.set_page_config(layout="wide", page_title="WATI Chat Manager")

# --- 2. CSS (FIXED FOR DARK MODE) ---
st.markdown("""
<style>
    .stDataFrame { width: 100%; }
    .block-container { padding-top: 2rem; padding-bottom: 2rem; }
    .stButton button { font-weight: bold; border-radius: 8px; }
    
    /* === CHAT BUBBLE STYLING === */
    .stChatMessage[data-testid="stChatMessage"]:has(div[aria-label="assistant"]) {
        background-color: #2e3b4e; 
        border: 1px solid #4a4a4a;
    }
    .stChatMessage[data-testid="stChatMessage"]:has(div[aria-label="user"]) {
        background-color: #0e76a8; 
        border: 1px solid #0e76a8;
        flex-direction: row-reverse;
    }
    .stChatMessage p, .stChatMessage div, .stChatMessage span {
        color: #ffffff !important;
    }
</style>
""", unsafe_allow_html=True)

# --- 3. CONNECTION ---
@st.cache_resource
def get_conn():
    # Try Cloud first, then local
    url = os.getenv("TURSO_DB_URL")
    token = os.getenv("TURSO_DB_TOKEN")
    
    if url and token:
        try:
            import libsql_experimental as libsql
            return libsql.connect(database=url, auth_token=token)
        except Exception:
            pass # Fallback silently

    local_db = "wati_chat_logs.db"
    if os.path.exists(local_db):
        import sqlite3
        return sqlite3.connect(local_db, check_same_thread=False)

    st.error("❌ Database Connection Failed.")
    st.stop()

conn = get_conn()

# --- 4. DATA FUNCTIONS (OPTIMIZED) ---

def run_query(query, params=()):
    try:
        cursor = conn.execute(query, params)
        if cursor.description:
            columns = [description[0] for description in cursor.description]
            data = cursor.fetchall()
            return pd.DataFrame(data, columns=columns)
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Query Error: {e}")
        return pd.DataFrame()

def extract_number(filename):
    if "-" in filename:
        return filename.split("-")[0]
    return filename.replace(".txt", "")

# CACHE: Only keep the CURRENT 500 rows in memory.
# TTL=600 means if you leave it for 10 mins, it clears to save RAM.
@st.cache_data(ttl=600, show_spinner=False)
def get_batch_users(offset, limit=500, search_term=None, hide_templates=False):
    
    sender_filter = "AND sender NOT LIKE '%Template%'" if hide_templates else ""
    
    if search_term:
        # Search Mode: Needs to scan more, but still paginated
        query = f"""
            SELECT 
                filename, 
                COUNT(*) as count, 
                MAX(timestamp) as last_active,
                (SELECT message_body FROM messages m2 
                 WHERE m2.filename = messages.filename 
                 ORDER BY timestamp DESC LIMIT 1) as preview,
                 (SELECT sender FROM messages m3 
                 WHERE m3.filename = messages.filename 
                 ORDER BY timestamp DESC LIMIT 1) as last_sender
            FROM messages
            WHERE filename IN (
                SELECT DISTINCT filename FROM messages 
                WHERE (message_body LIKE ? OR filename LIKE ?)
                {sender_filter}
            )
            GROUP BY filename
            ORDER BY last_active DESC
            LIMIT ? OFFSET ?
        """
        params = (f"%{search_term}%", f"%{search_term}%", limit, offset)
    else:
        # BROWSE MODE: Restricted to Last 30 Days for Speed
        # This prevents the DB from grouping 5 years of data every time
        query = f"""
            SELECT 
                filename, 
                COUNT(*) as count, 
                MAX(timestamp) as last_active,
                (SELECT message_body FROM messages m2 
                 WHERE m2.filename = messages.filename 
                 ORDER BY timestamp DESC LIMIT 1) as preview,
                 (SELECT sender FROM messages m3 
                 WHERE m3.filename = messages.filename 
                 ORDER BY timestamp DESC LIMIT 1) as last_sender
            FROM messages 
            WHERE timestamp > date('now', '-30 days') 
            {sender_filter}
            GROUP BY filename 
            ORDER BY last_active DESC
            LIMIT ? OFFSET ?
        """
        params = (limit, offset)
        
    df = run_query(query, params)
    
    if not df.empty:
        df['Phone'] = df['filename'].apply(extract_number)
        df['Last Active'] = pd.to_datetime(df['last_active'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M')
        return df[['Phone', 'last_sender', 'preview', 'Last Active', 'filename', 'count']] 
    return pd.DataFrame()

def get_full_chat_history(filename, hide_templates=False):
    query = "SELECT timestamp, message_body, sender FROM messages WHERE filename = ?"
    if hide_templates:
        query += " AND sender NOT LIKE '%Template%'"
    query += " ORDER BY timestamp ASC"
    return run_query(query, (filename,))

def get_bulk_export_data(filenames, hide_templates=False):
    # This might take a moment, so we don't cache it to avoid memory spikes
    placeholders = ','.join('?' for _ in filenames)
    query = f"SELECT filename, timestamp, sender, message_body FROM messages WHERE filename IN ({placeholders})"
    if hide_templates:
        query += " AND sender NOT LIKE '%Template%'"
    query += " ORDER BY filename, timestamp ASC"
    
    df = run_query(query, tuple(filenames))
    if not df.empty:
        df['Phone'] = df['filename'].apply(extract_number)
        return df[['Phone', 'timestamp', 'sender', 'message_body']]
    return df

# --- 5. STATE & SIDEBAR ---
if 'view_mode' not in st.session_state: st.session_state.view_mode = "list" 
if 'page_number' not in st.session_state: st.session_state.page_number = 0
if 'selected_file' not in st.session_state: st.session_state.selected_file = None
if 'clean_phone' not in st.session_state: st.session_state.clean_phone = ""

# --- SIDEBAR ---
with st.sidebar:
    st.header("⚙️ Filters")
    hide_templates = st.checkbox("Hide Templates / Bots", value=False)
    st.markdown("---")
    st.caption(f"Page: {st.session_state.page_number + 1}")

# --- 6. APP LOGIC ---

# === VIEW 1: MAIN DASHBOARD ===
if st.session_state.view_mode == "list":
    st.title("📂 WATI Chat Manager")
    
    col_search, col_prev, col_stat, col_next = st.columns([4, 1, 2, 1])
    with col_search:
        search_query = st.text_input("🔍 Search", placeholder="Phone or Keyword...", label_visibility="collapsed")
    
    # RESET PAGE ON SEARCH
    if search_query and 'last_search' in st.session_state and st.session_state.last_search != search_query:
        st.session_state.page_number = 0
        st.session_state.last_search = search_query
        
    BATCH_SIZE = 500  # <--- UPDATED TO 500
    current_offset = st.session_state.page_number * BATCH_SIZE

    # NEXT / PREV BUTTONS
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
        st.markdown(f"<div style='text-align: center; padding-top: 10px; color: #666;'>Row {current_offset} - {current_offset + BATCH_SIZE}</div>", unsafe_allow_html=True)

    # 1. GET DATA (Using Cache)
    with st.spinner('Loading users...'):
        batch_df = get_batch_users(current_offset, BATCH_SIZE, search_query, hide_templates)

    if batch_df.empty:
        st.info("No active conversations found in the last 30 days.")
    else:
        # --- EXPORT 500 BATCH ---
        with st.expander(f"📥 Export Current Batch ({len(batch_df)} Users)", expanded=False):
            if st.button("Download Batch CSV"):
                with st.spinner("Preparing CSV..."):
                    export_df = get_bulk_export_data(batch_df['filename'].tolist(), hide_templates)
                    csv = export_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📄 Click to Download CSV",
                        data=csv,
                        file_name=f"wati_batch_{st.session_state.page_number}.csv",
                        mime="text/csv"
                    )

        # RENDER TABLE
        event = st.dataframe(
            batch_df[['Phone', 'last_sender', 'preview', 'Last Active']],
            column_config={
                "Phone": st.column_config.TextColumn("Contact", width="medium"),
                "last_sender": st.column_config.TextColumn("Last Sender", width="small"),
                "preview": st.column_config.TextColumn("Latest Message", width="large"),
                "Last Active": st.column_config.TextColumn("Last Active", width="small"),
            },
            use_container_width=True,
            hide_index=True,
            selection_mode="single-row",
            on_select="rerun",
            height=600
        )
        if len(event.selection.rows) > 0:
            index = event.selection.rows[0]
            st.session_state.selected_file = batch_df.iloc[index]['filename']
            st.session_state.clean_phone = batch_df.iloc[index]['Phone']
            st.session_state.view_mode = "chat"
            st.rerun()

# === VIEW 2: CHAT DETAIL ===
elif st.session_state.view_mode == "chat":
    col_back, col_title, col_dl = st.columns([1, 7, 2])
    
    with col_back:
        if st.button("⬅️ Back"):
            st.session_state.view_mode = "list"
            st.session_state.selected_file = None
            st.rerun()
            
    with col_title:
        st.subheader(f"💬 {st.session_state.clean_phone}")

    # Load Chat
    chat_df = get_full_chat_history(st.session_state.selected_file, hide_templates)
    
    with col_dl:
        if not chat_df.empty:
            csv = chat_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Export Chat CSV",
                data=csv,
                file_name=f"chat_{st.session_state.clean_phone}.csv",
                mime="text/csv"
            )

    st.markdown("---")
    
    if chat_df.empty:
        st.warning("No messages found.")
    
    with st.container():
        for _, row in chat_df.iterrows():
            sender = row['sender']
            is_bot = 'Template' in sender or 'System' in sender
            
            if is_bot:
                with st.chat_message("assistant"):
                    st.write(row['message_body'])
                    st.caption(f"🤖 {sender} • {row['timestamp']}")
            else:
                with st.chat_message("user"):
                    st.write(row['message_body'])
                    st.caption(f"👤 {sender} • {row['timestamp']}")