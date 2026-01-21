import streamlit as st
import pandas as pd
import os
import time

# --- 1. PAGE CONFIG ---
st.set_page_config(layout="wide", page_title="WATI Chat Manager")

# --- 2. CSS ---
st.markdown("""
<style>
    .stDataFrame { width: 100%; }
    .block-container { padding-top: 2rem; padding-bottom: 2rem; }
    .stButton button { font-weight: bold; border-radius: 8px; }
    
    /* Dark Mode Bubbles */
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
    url = os.getenv("TURSO_DB_URL")
    token = os.getenv("TURSO_DB_TOKEN")
    if url and token:
        try:
            import libsql_experimental as libsql
            return libsql.connect(database=url, auth_token=token)
        except: pass
    
    if os.path.exists("wati_chat_logs.db"):
        import sqlite3
        return sqlite3.connect("wati_chat_logs.db", check_same_thread=False)
    
    st.error("❌ No DB Connection.")
    st.stop()

conn = get_conn()

# --- 4. OPTIMIZED DATA FUNCTIONS ---

def run_query(query, params=()):
    try:
        cursor = conn.execute(query, params)
        if cursor.description:
            columns = [description[0] for description in cursor.description]
            return pd.DataFrame(cursor.fetchall(), columns=columns)
        return pd.DataFrame()
    except Exception as e:
        return pd.DataFrame()

def extract_number(filename):
    if "-" in filename: return filename.split("-")[0]
    return filename.replace(".txt", "")

# 🔥 NEW STRATEGY: Fetch RAW rows and Group in Python
# This avoids the slow "GROUP BY" scan in the cloud DB.
@st.cache_data(ttl=300, show_spinner=False)
def get_recent_users(limit=2000, search_term=None):
    
    if search_term:
        # Search is specific, so we can afford a deeper scan
        query = """
            SELECT DISTINCT filename, timestamp, message_body, sender
            FROM messages 
            WHERE filename LIKE ? OR message_body LIKE ?
            ORDER BY timestamp DESC LIMIT ?
        """
        params = (f"%{search_term}%", f"%{search_term}%", limit)
    else:
        # FAST PATH: Just grab latest messages
        # "Give me the last 2000 messages instantly"
        query = """
            SELECT filename, timestamp, message_body, sender
            FROM messages 
            ORDER BY timestamp DESC LIMIT ?
        """
        params = (limit,)

    df = run_query(query, params)
    
    if df.empty: return pd.DataFrame()

    # --- PYTHON PROCESSING (Fast) ---
    # We now have ~2000 rows. We just want unique filenames (users).
    # keep='first' ensures we keep the most recent message for that user.
    users = df.drop_duplicates(subset=['filename'], keep='first').copy()
    
    # Clean up for display
    users['Phone'] = users['filename'].apply(extract_number)
    users['Last Active'] = pd.to_datetime(users['timestamp'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M')
    users['preview'] = users['message_body']
    users['last_sender'] = users['sender']
    
    # Return formatted table
    return users[['Phone', 'last_sender', 'preview', 'Last Active', 'filename']]

def get_full_chat(filename, hide_bot=False):
    query = "SELECT timestamp, message_body, sender FROM messages WHERE filename = ?"
    if hide_bot: query += " AND sender NOT LIKE '%Template%'"
    query += " ORDER BY timestamp ASC"
    return run_query(query, (filename,))

def get_bulk_export(filenames):
    placeholders = ','.join('?' for _ in filenames)
    query = f"SELECT filename, timestamp, sender, message_body FROM messages WHERE filename IN ({placeholders}) ORDER BY filename, timestamp ASC"
    df = run_query(query, tuple(filenames))
    if not df.empty: df['Phone'] = df['filename'].apply(extract_number)
    return df

# --- 5. APP LOGIC ---

if 'view_mode' not in st.session_state: st.session_state.view_mode = "list"
if 'selected_file' not in st.session_state: st.session_state.selected_file = None

# --- SIDEBAR ---
with st.sidebar:
    st.header("⚡ Filters")
    hide_templates = st.checkbox("Hide Templates (Chat View)", value=False)
    # Reload button to clear cache
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

# === MAIN LIST ===
if st.session_state.view_mode == "list":
    st.title("📂 WATI Chat Manager")

    search = st.text_input("🔍 Search", placeholder="Phone number...")
    
    with st.spinner("Fetching latest conversations..."):
        # We fetch 2000 raw messages -> roughly 200-500 unique active users
        df = get_recent_users(limit=2000, search_term=search)

    if df.empty:
        st.info("No messages found.")
    else:
        st.caption(f"Showing {len(df)} most recently active users.")
        
        # Batch Export
        with st.expander("📥 Export These Users"):
            if st.button("Download CSV"):
                export_df = get_bulk_export(df['filename'].tolist())
                st.download_button("📄 Download", export_df.to_csv(index=False).encode('utf-8'), "wati_export.csv")

        event = st.dataframe(
            df[['Phone', 'last_sender', 'preview', 'Last Active']],
            column_config={
                "Phone": st.column_config.TextColumn("Contact", width="medium"),
                "last_sender": "Sender",
                "preview": st.column_config.TextColumn("Latest Message", width="large"),
                "Last Active": "Time",
            },
            use_container_width=True, 
            hide_index=True,
            selection_mode="single-row",
            on_select="rerun",
            height=600
        )
        if len(event.selection.rows) > 0:
            st.session_state.selected_file = df.iloc[event.selection.rows[0]]['filename']
            st.session_state.clean_phone = df.iloc[event.selection.rows[0]]['Phone']
            st.session_state.view_mode = "chat"
            st.rerun()

# === CHAT VIEW ===
elif st.session_state.view_mode == "chat":
    col1, col2 = st.columns([1, 8])
    if col1.button("⬅️ Back"):
        st.session_state.view_mode = "list"
        st.rerun()
    col2.subheader(f"💬 {st.session_state.clean_phone}")

    chat_df = get_full_chat(st.session_state.selected_file, hide_templates)
    
    if not chat_df.empty:
        st.download_button("📥 Export Chat", chat_df.to_csv(index=False).encode('utf-8'), "chat.csv")
    
    st.markdown("---")
    for _, row in chat_df.iterrows():
        is_bot = 'Template' in row['sender'] or 'System' in row['sender']
        role, av = ("assistant", "🤖") if is_bot else ("user", "👤")
        with st.chat_message(role):
            st.write(row['message_body'])
            st.caption(f"{av} {row['sender']} • {row['timestamp']}")