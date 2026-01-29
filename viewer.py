import streamlit as st
import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta, time
import hashlib
import base64

# --- 1. CONFIGURATION ---
st.set_page_config(layout="wide", page_title="Rocket Health QA Dashboard")

# --- 2. ADVANCED CSS STYLING ---
st.markdown("""
<style>
    /* GLOBAL CLEANUP */
    .stChatFloatingInputContainer { bottom: 20px; }
    
    /* === 1. TEAM & BOT MESSAGES (RIGHT SIDE) === */
    /* Container: Flips avatar to the right */
    .stChatMessage[data-testid="stChatMessage"]:has(div[aria-label="assistant"]) {
        flex-direction: row-reverse;
        text-align: right;
    }
    /* Bubble Styling */
    .stChatMessage[data-testid="stChatMessage"]:has(div[aria-label="assistant"]) .stChatMessageContent {
        background: linear-gradient(135deg, #005a9e 0%, #004170 100%); /* Professional Blue Gradient */
        color: #ffffff;
        border-radius: 12px 2px 12px 12px; /* Sharp top-right corner */
        text-align: left;
        box-shadow: 0 2px 5px rgba(0,0,0,0.2);
        border: 1px solid #003366;
        min-width: 200px;
    }
    /* Sender Name Styling (Inside Bubble) */
    .stChatMessage[data-testid="stChatMessage"]:has(div[aria-label="assistant"]) .sender-name {
        font-size: 0.75rem;
        color: #a6d5fa; /* Light Blue for name */
        font-weight: bold;
        margin-bottom: 4px;
        display: block;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }

    /* === 2. PATIENT MESSAGES (LEFT SIDE) === */
    /* Container: Standard Left Alignment */
    .stChatMessage[data-testid="stChatMessage"]:has(div[aria-label="user"]) {
        flex-direction: row;
    }
    /* Bubble Styling */
    .stChatMessage[data-testid="stChatMessage"]:has(div[aria-label="user"]) .stChatMessageContent {
        background-color: #2b2d31; /* Dark Grey (Discord-like) */
        color: #e0e0e0;
        border-radius: 2px 12px 12px 12px; /* Sharp top-left corner */
        box-shadow: 0 1px 3px rgba(0,0,0,0.3);
        border: 1px solid #3e4045;
        min-width: 200px; /* Ensure wide enough for name */
    }
    /* Sender Name Styling (Inside Bubble) */
    .stChatMessage[data-testid="stChatMessage"]:has(div[aria-label="user"]) .sender-name {
        font-size: 0.75rem;
        color: #b0b0b0; /* Dim grey for name */
        font-weight: bold;
        margin-bottom: 4px;
        display: block;
    }

    /* === 3. BADGES & UI ELEMENTS === */
    .team-badge {
        padding: 4px 10px;
        border-radius: 12px;
        color: white;
        font-weight: 600;
        font-size: 0.75rem;
        margin-right: 5px;
        margin-bottom: 5px;
        display: inline-block;
        border: 1px solid rgba(255,255,255,0.1);
    }
    .date-separator {
        text-align: center;
        margin: 25px 0 15px 0;
        position: relative;
    }
    .date-separator span {
        background-color: #1e1e1e;
        padding: 4px 12px;
        border-radius: 12px;
        color: #888;
        font-size: 0.8rem;
        font-weight: 500;
        border: 1px solid #333;
    }
</style>
""", unsafe_allow_html=True)

try:
    MONGO_URI = st.secrets["MONGO_URI"]
except:
    MONGO_URI = "mongodb+srv://googlemcp:lookism24@cluster.idt477b.mongodb.net/?appName=Cluster"

# --- 3. HELPER FUNCTIONS ---

@st.cache_resource
def get_collection():
    try:
        client = MongoClient(MONGO_URI)
        client.admin.command('ping')
        return client["wati_logs"]["messages"]
    except Exception as e:
        st.error(f"❌ Connection Error: {e}")
        st.stop()

collection = get_collection()

@st.cache_data(ttl=3600)
def get_all_senders():
    """Fetches all unique senders from the DB to populate filters dynamically."""
    senders = collection.distinct("sender")
    return [s for s in senders if s and s not in ["Template", "System", "Bot"]]

def get_color_for_name(name):
    """Generates a consistent color based on the name string."""
    colors = ["#FF6B6B", "#4ECDC4", "#45B7D1", "#FFA07A", "#98D8C8", "#F7DC6F", "#BB8FCE", "#F1948A", "#5DADE2"]
    hash_val = int(hashlib.sha256(name.encode('utf-8')).hexdigest(), 16)
    return colors[hash_val % len(colors)]

def create_avatar_svg(initial, color):
    """Creates a custom SVG avatar with the specific assigned color."""
    svg = f"""
    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100" width="100" height="100">
      <circle cx="50" cy="50" r="50" fill="{color}" />
      <text x="50" y="55" font-family="Arial" font-size="45" fill="white" text-anchor="middle" alignment-baseline="middle" font-weight="bold">{initial}</text>
    </svg>
    """
    b64 = base64.b64encode(svg.encode('utf-8')).decode("utf-8")
    return f"data:image/svg+xml;base64,{b64}"

# --- 4. DATA LOGIC ---

def get_data_qa(limit, search_term, hide_templates, staff_list, filter_teammate, patient_mode_on, date_range):
    match_stage = {}
    
    start_dt = datetime.combine(date_range[0], time.min)
    end_dt = datetime.combine(date_range[1], time.max)
    match_stage["timestamp"] = {"$gte": start_dt, "$lte": end_dt}

    if hide_templates:
        match_stage["sender"] = {"$ne": "Template"}

    if search_term:
        match_stage["$or"] = [
            {"filename": {"$regex": search_term, "$options": "i"}},
            {"message_body": {"$regex": search_term, "$options": "i"}}
        ]

    if filter_teammate:
        touched_files = collection.distinct("filename", {"sender": filter_teammate})
        match_stage["filename"] = {"$in": touched_files}

    if patient_mode_on and staff_list:
        if "sender" in match_stage and isinstance(match_stage["sender"], dict):
            match_stage["sender"]["$nin"] = staff_list
        else:
            match_stage["sender"] = {"$nin": staff_list}

    pipeline = [
        {"$match": match_stage},
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
    if not results: return pd.DataFrame()
        
    df = pd.DataFrame(results)
    df.rename(columns={"_id": "filename", "last_msg": "preview"}, inplace=True)
    df['Phone'] = df['filename'].apply(lambda x: x.split('-')[0] if '-' in x else x)
    return df

def get_chat_history(filename):
    return pd.DataFrame(list(collection.find({"filename": filename}).sort("timestamp", 1)))

# --- 5. UI LAYOUT ---

if 'view_mode' not in st.session_state: st.session_state.view_mode = "list"
if 'selected_file' not in st.session_state: st.session_state.selected_file = None

# === SIDEBAR ===
with st.sidebar:
    st.title("🎛️ QA Controls")
    
    today = datetime.today()
    d_range = st.date_input("Date Range", (today - timedelta(days=30), today))
    if isinstance(d_range, tuple) and len(d_range) == 2:
        start_date, end_date = d_range
    else:
        start_date, end_date = today, today

    st.markdown("---")
    
    # --- HARDCODED STAFF LIST ---
    st.caption("👥 **Identify Staff Members**")
    
    all_senders = get_all_senders()
    
    # 1. System/Bots (Auto-detected)
    system_bots = [s for s in all_senders if any(k in s for k in ["Template", "System", "Rocket", "Bot"])]
    
    # 2. YOUR HARDCODED TEAM (Exact names)
    manual_team = ["Hamood .", "Moomal Kumari", "Shankar :)", "Apoorva Nair"]
    
    # 3. Combine both for the default selection
    # We use set() to avoid duplicates if a name appears in both lists
    default_staff = list(set(system_bots + [m for m in manual_team if m in all_senders]))
    
    staff_list = st.multiselect(
        "Select Team/Bots (to distinguish from patients):", 
        options=all_senders, 
        default=default_staff
    )
    
    if staff_list:
        html_badges = "<div style='margin-bottom: 20px;'>"
        for name in staff_list:
            color = get_color_for_name(name)
            display_name = name.split()[0] if " " in name else name
            html_badges += f"<span class='team-badge' style='background-color:{color}'>{display_name}</span>"
        html_badges += "</div>"
        st.markdown(html_badges, unsafe_allow_html=True)
    
    st.markdown("---")
    limit = st.slider("Rows to load", 50, 500, 100)
    hide_tmps = st.checkbox("Hide Templates", value=True)
    
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

# === VIEW 1: LIST ===
if st.session_state.view_mode == "list":
    st.header("📊 Conversations & Queries")
    
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        search = st.text_input("🔍 Search User...", placeholder="Phone, Name, or Message content")
    with col2:
        teammate_options = ["All"] + staff_list
        selected_teammate = st.selectbox("👮 Filter by Teammate", teammate_options)
        filter_tm = None if selected_teammate == "All" else selected_teammate
    with col3:
        patient_mode = st.toggle("🗣️ Patient Queries Only")
        st.caption("Showing *Patient* msgs only" if patient_mode else "Showing *Absolute* last msg")

    with st.spinner("Crunching QA numbers..."):
        df = get_data_qa(limit, search, hide_tmps, staff_list, filter_tm, patient_mode, (start_date, end_date))

    if df.empty:
        st.info("No conversations found.")
    else:
        def get_status_badge(count):
            return "🆕 New Lead" if count <= 5 else "🔄 Recurring"
        df['Status'] = df['msg_count'].apply(get_status_badge)

        st.write(f"Found **{len(df)}** active conversations.")
        
        event = st.dataframe(
            df[['Status', 'Phone', 'last_sender', 'preview', 'last_active']],
            column_config={
                "Status": st.column_config.TextColumn("Type", width="small"),
                "Phone": st.column_config.TextColumn("User / Phone", width="medium"),
                "last_sender": st.column_config.TextColumn("Last Sender", width="small"),
                "preview": st.column_config.TextColumn("Message Preview", width="large"),
                "last_active": st.column_config.DatetimeColumn("Time", format="D MMM, HH:mm"),
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
    col1, col2 = st.columns([1, 8])
    with col1:
        if st.button("⬅️ Back"):
            st.session_state.view_mode = "list"
            st.rerun()
    with col2:
        st.subheader(f"💬 {st.session_state.clean_phone}")
    
    chat_df = get_chat_history(st.session_state.selected_file)
    show_system = st.checkbox("Show System/Template Messages", value=False)
    
    chat_container = st.container()
    with chat_container:
        prev_date = None
        
        for _, row in chat_df.iterrows():
            sender = row.get('sender', 'System')
            msg = row.get('message_body', '')
            ts = row.get('timestamp', '')
            
            # 1. IDENTIFY ROLE (Dynamic Logic)
            is_bot = any(k in sender for k in ["Template", "System", "Bot"])
            # Check if in selected staff list OR is a bot
            is_staff = sender in staff_list or is_bot
            
            # FILTER
            if is_bot and not show_system:
                continue

            # 2. DATE HEADER
            if isinstance(ts, datetime):
                msg_date = ts.date()
                if msg_date != prev_date:
                    st.markdown(f"<div class='date-separator'><span>{msg_date.strftime('%B %d, %Y')}</span></div>", unsafe_allow_html=True)
                    prev_date = msg_date
                time_str = ts.strftime("%H:%M")
            else:
                time_str = ""

            # 3. ASSIGN VISUALS
            if is_staff:
                role = "assistant"
                assigned_color = get_color_for_name(sender)
                initial = sender[0].upper() if sender else "R"
                avatar_icon = create_avatar_svg(initial, assigned_color)
            else:
                role = "user"
                avatar_icon = "👤"

            # 4. RENDER BUBBLE
            with st.chat_message(role, avatar=avatar_icon):
                st.markdown(f"<span class='sender-name'>{sender}</span>", unsafe_allow_html=True)
                st.write(msg)
                st.caption(time_str)