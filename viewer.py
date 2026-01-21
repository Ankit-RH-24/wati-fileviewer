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
    
    /* === CHAT BUBBLE STYLING (High Contrast) === */
    
    /* 🤖 Bot/Template: Dark Gray Background, White Text */
    .stChatMessage[data-testid="stChatMessage"]:has(div[aria-label="assistant"]) {
        background-color: #2e3b4e; 
        border: 1px solid #4a4a4a;
    }
    
    /* 👤 User: Blue Background, White Text */
    .stChatMessage[data-testid="stChatMessage"]:has(div[aria-label="user"]) {
        background-color: #0e76a8; 
        border: 1px solid #0e76a8;
        flex-direction: row-reverse; /* Align to right */
    }
    
    /* FORCE TEXT COLOR TO WHITE for visibility in Dark Mode */
    .stChatMessage p, .stChatMessage div, .stChatMessage span {
        color: #ffffff !important;
    }
    
    /* Add subtle shadow to bubbles */
    .stChatMessage {
        box-shadow: 0 2px 5px rgba(0,0,0,0.2);
    }
</style>
""", unsafe_allow_html=True)

# --- 3. HYBRID CONNECTION ---
@st.cache_resource
def get_conn():
    # A. Cloud (Turso)
    url = os.getenv("TURSO_DB_URL")
    token = os.getenv("TURSO_DB_TOKEN")
    
    if url and token:
        try:
            import libsql_experimental as libsql
            return libsql.connect(database=url, auth_token=token)
        except Exception as e:
            st.warning(f"⚠️ Cloud DB Error: {e}. Trying local...")

    # B. Local File
    local_db = "wati_chat_logs.db"
    if os.path.exists(local_db):
        import sqlite3
        return sqlite3.connect(local_db, check_same_thread=False)

    st.error("❌ No Database found! Set Secrets or keep 'wati_chat_logs.db' locally.")
    st.stop()

conn = get_conn()

# --- 4. DATA FUNCTIONS ---

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

def get_batch_users(offset, limit=100, search_term=None, hide_templates=False):
    """
    Fetches the list of users. 
    If hide_templates=True, we try to find users who have sent at least one real message.
    """
    
    sender_filter = "AND sender NOT LIKE '%Template%'" if hide_templates else ""
    
    if search_term:
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
        # Optimization: We just grab the latest active users
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
            WHERE 1=1 {sender_filter}
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
    """
    Fetches FULL chat history for a list of filenames (for the CSV export).
    """
    placeholders = ','.join('?' for _ in filenames)
    query = f"SELECT filename, timestamp, sender, message_body FROM messages WHERE filename IN ({placeholders})"
    
    if hide_templates:
        query += " AND sender NOT LIKE '%Template%'"
        
    query += " ORDER BY filename, timestamp ASC"
    
    df = run_query(query, tuple(filenames))
    # Clean up filename to phone for the CSV
    if not df.empty:
        df['Phone'] = df['filename'].apply(extract_number)
        return df[['Phone', 'timestamp', 'sender', 'message_body']]
    return df

# --- 5. STATE & SIDEBAR ---
if 'view_mode' not in st.session_state: st.session_state.view_mode = "list" 
if 'page_number' not in st.session_state: st.session_state.page_number = 0
if 'selected_file' not in st.session_state: st.session_state.selected_file = None
if 'clean_phone' not in st.session_state: st.session_state.clean_phone = ""

# --- SIDEBAR FILTERS ---
with st.sidebar:
    st.header("⚙️ Filters")
    st.write("Control what you see and export.")
    
    # Filter: Hide Templates
    hide_templates = st.checkbox("Hide Templates / Bots", value=False, help="Hides messages sent by 'Template' or system bots.")
    
    st.markdown("---")
    st.caption(f"Page: {st.session_state.page_number + 1}")

# --- 6. APP LOGIC ---

# === VIEW 1: MAIN DASHBOARD ===
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

    # 1. Get the List of Users
    batch_df = get_batch_users(current_offset, BATCH_SIZE, search_query, hide_templates)

    if batch_df.empty:
        st.info("No conversations found. Try changing filters.")
    else:
        # --- NEW: BATCH EXPORT BUTTON ---
        # Logic: We take the filenames from the current view and fetch their content
        with st.expander("📥 Export Options", expanded=False):
            st.write(f"Exporting data for these {len(batch_df)} users...")
            if st.button("Download Batch CSV"):
                # Fetch full content for these users
                export_df = get_bulk_export_data(batch_df['filename'].tolist(), hide_templates)
                
                # Convert to CSV
                csv = export_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📄 Click to Download CSV",
                    data=csv,
                    file_name=f"wati_batch_export_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv"
                )

        # Render Table
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

    # Fetch Data (Respecting Sidebar Filter)
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
        st.warning("No messages found with current filters (try unchecking 'Hide Templates').")
    
    # Render Chat
    with st.container():
        for _, row in chat_df.iterrows():
            sender = row['sender']
            msg = row['message_body']
            time = row['timestamp']
            
            # Visual Logic: 
            # If sender contains "Template", "System", or is generic -> Robot Side
            # Else -> User Side
            is_bot = 'Template' in sender or 'System' in sender
            
            if is_bot:
                with st.chat_message("assistant"):
                    st.write(msg)
                    st.caption(f"🤖 {sender} • {time}")
            else:
                with st.chat_message("user"):
                    st.write(msg)
                    st.caption(f"👤 {sender} • {time}")