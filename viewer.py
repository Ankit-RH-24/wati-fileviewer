import streamlit as st
import pandas as pd
import libsql_experimental as libsql
import os

# --- PAGE CONFIGURATION ---
st.set_page_config(layout="wide", page_title="WATI Chat Manager")

# --- 2. CSS FOR VISIBILITY & LAYOUT ---
st.markdown("""
<style>
    /* Full width table */
    .stDataFrame { width: 100%; }
    
    /* Push content down so buttons aren't cut off */
    .block-container { 
        padding-top: 4rem; 
        padding-bottom: 2rem; 
    }
    
    /* Back Button Styling */
    .stButton button {
        font-weight: bold;
        border-radius: 8px;
    }
    
    /* --- FIX: FORCE DARK TEXT IN CHAT BUBBLES --- */
    /* This ensures text is black even if your Mac is in Dark Mode */
    .stChatMessage {
        background-color: #f8f9fa; 
        color: #262730 !important; /* Dark Charcoal Text */
        border: 1px solid #e9ecef;
        border-radius: 12px;
        padding: 15px;
        margin-bottom: 10px;
    }
    
    /* Force inner text elements (paragraphs, markdown) to be dark too */
    .stChatMessage p, .stChatMessage div, .stChatMessage span, .stChatMessage strong {
        color: #262730 !important;
    }
</style>
""", unsafe_allow_html=True)

# --- 3. DATABASE CONNECTION ---
@st.cache_resource
def get_conn():
    # We get these secrets from Vercel/Streamlit Cloud environment variables
    url = os.getenv("TURSO_DB_URL")
    token = os.getenv("TURSO_DB_TOKEN")

    if not url or not token:
        st.error("❌ Missing Database Credentials. Please set TURSO_DB_URL and TURSO_DB_TOKEN environment variables.")
        st.stop()

    return libsql.connect(database=url, auth_token=token)

conn = get_conn()

# --- 5. SESSION STATE ---
if 'view_mode' not in st.session_state:
    st.session_state.view_mode = "list" 
if 'page_number' not in st.session_state:
    st.session_state.page_number = 0
if 'selected_file' not in st.session_state:
    st.session_state.selected_file = None
if 'clean_phone' not in st.session_state:
    st.session_state.clean_phone = ""

# --- 6. DATA FUNCTIONS ---

def get_batch_users(offset, limit=100, search_term=None):
    """Fetch user list + Latest Message Preview"""
    if search_term:
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
        query = """
            SELECT 
                filename, 
                COUNT(*) as count, 
                MAX(timestamp) as last_active,
                (SELECT message_body FROM messages m2 
                 WHERE m2.filename = messages.filename 
                 ORDER BY timestamp DESC LIMIT 1) as preview
            FROM messages 
            GROUP BY filename 
            ORDER BY last_active DESC
            LIMIT ? OFFSET ?
        """
        params = (limit, offset)
        
    df = pd.read_sql(query, conn, params=params)
    
    if not df.empty:
        df['Phone'] = df['filename'].apply(extract_number)
        df['Last Active'] = pd.to_datetime(df['last_active']).dt.strftime('%Y-%m-%d %H:%M')
        return df[['Phone', 'count', 'preview', 'Last Active', 'filename']] 
    return pd.DataFrame()

def get_full_chat_history(filename):
    query = """
        SELECT timestamp, message_body 
        FROM messages 
        WHERE filename = ? 
        ORDER BY timestamp ASC
    """
    return pd.read_sql(query, conn, params=(filename,))

# --- 7. MAIN APP LOGIC ---

# === VIEW 1: DASHBOARD LIST ===
if st.session_state.view_mode == "list":
    st.title("📂 WATI Chat Manager")

    # Controls: Search | Prev | Stats | Next
    col_search, col_prev, col_stat, col_next = st.columns([4, 1, 2, 1])
    
    with col_search:
        search_query = st.text_input("🔍 Search", placeholder="Phone or Keyword...", label_visibility="collapsed")
    
    # Reset pagination if searching
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
        st.markdown(f"<div style='text-align: center; padding-top: 10px; color: #666; font-size: 0.9em;'>Rows {current_offset} - {current_offset + BATCH_SIZE}</div>", unsafe_allow_html=True)

    # Render Table
    batch_df = get_batch_users(current_offset, BATCH_SIZE, search_query)

    if batch_df.empty:
        st.info("No users found.")
    else:
        event = st.dataframe(
            batch_df[['Phone', 'count', 'preview', 'Last Active']],
            column_config={
                "Phone": st.column_config.TextColumn("Contact", width="medium"),
                "count": st.column_config.NumberColumn("No of Messages", width="small"),
                "preview": st.column_config.TextColumn("Latest Message", width="large"),
                "Last Active": st.column_config.TextColumn("Last Active", width="small"),
            },
            use_container_width=True,
            hide_index=True,
            selection_mode="single-row",
            on_select="rerun",
            height=700
        )

        # Handle Click
        if len(event.selection.rows) > 0:
            index = event.selection.rows[0]
            st.session_state.selected_file = batch_df.iloc[index]['filename']
            st.session_state.clean_phone = batch_df.iloc[index]['Phone']
            st.session_state.view_mode = "chat"
            st.rerun()

# === VIEW 2: FULL CHAT ===
elif st.session_state.view_mode == "chat":
    
    # Back Button & Header
    col_back, col_title = st.columns([1, 10])
    with col_back:
        if st.button("⬅️ Back"):
            st.session_state.view_mode = "list"
            st.session_state.selected_file = None
            st.rerun()
            
    with col_title:
        st.subheader(f"💬 Conversation with {st.session_state.clean_phone}")

    # Load Chat
    chat_df = get_full_chat_history(st.session_state.selected_file)
    
    st.markdown("---")
    
    # Display Bubbles
    with st.container():
        for _, row in chat_df.iterrows():
            with st.chat_message("assistant"):
                st.write(row['message_body'])
                st.caption(f"{row['timestamp']}")