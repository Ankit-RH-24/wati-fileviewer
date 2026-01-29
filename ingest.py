import os
import sqlite3
import re
from datetime import datetime

# --- CONFIGURATION ---
# UPDATE THIS PATH to your folder containing the .txt files
FOLDER_PATH = "."  # Use "." if running from inside the folder, or put the full path
DB_PATH = "wati_chat_logs.db"

# --- DATABASE SETUP ---
if os.path.exists(DB_PATH):
    os.remove(DB_PATH) # Start fresh

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# Enable Speed Mode
c.execute("PRAGMA journal_mode=WAL;")
c.execute("PRAGMA synchronous=NORMAL;")

c.execute('''
    CREATE TABLE IF NOT EXISTS messages (
        filename TEXT,
        sender TEXT,
        message_body TEXT,
        timestamp TEXT,
        status TEXT
    )
''')

# --- PARSING LOGIC ---
def process_message_block(filename, raw_timestamp, full_text):
    """
    Decides if a full block of text is a Template, User, or System message.
    """
    # 1. Standardize Time
    try:
        # Format: 09/26/2025 17:52:14
        dt = datetime.strptime(raw_timestamp, "%m/%d/%Y %H:%M:%S")
        iso_time = dt.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        iso_time = raw_timestamp

    # 2. Identify Sender
    clean_text = full_text.strip()
    
    # CASE A: Template (Starts with 'Template "')
    # We check startswith because templates can be huge and multi-line.
    if clean_text.startswith('Template "'):
        # Try to extract content inside quotes if possible, otherwise keep all
        # Regex to capture: Template "CONTENT" ...
        # We use DOTALL to allow (.) to match newlines
        match = re.search(r'^Template\s+"(.*?)"(?:\s+was sent\.|$)', clean_text, re.DOTALL)
        if match:
            return (filename, 'Template', match.group(1), iso_time, 'sent')
        else:
            # Fallback if regex misses end quote
            return (filename, 'Template', clean_text, iso_time, 'sent')

    # CASE B: User / Name (Contains ": ")
    # Format: "Anoop Pakki: It works" or "Shankar :): Thanks"
    # We split ONLY on the FIRST ": " found.
    if ": " in clean_text:
        parts = clean_text.split(": ", 1)
        sender_name = parts[0].strip()
        body = parts[1].strip()
        
        # Cleanup: If sender is "Bot", treat appropriately (optional)
        return (filename, sender_name, body, iso_time, 'received')

    # CASE C: System / Fallback
    return (filename, 'System', clean_text, iso_time, 'system')

# --- MAIN LOOP ---
print("🚀 Starting Smart Buffer Ingestion...")
batch_data = []
total_count = 0
user_msg_count = 0

# Regex to find start of message: [09/26/2025 17:52:14]
timestamp_pattern = re.compile(r'^\[(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2})\]\s+(.*)')

with os.scandir(FOLDER_PATH) as entries:
    for entry in entries:
        if entry.name.endswith(".txt") and "requirements" not in entry.name:
            try:
                with open(entry.path, "r", encoding="utf-8", errors="ignore") as f:
                    
                    # Buffer variables
                    current_timestamp = None
                    current_text_buffer = []
                    
                    for line in f:
                        match = timestamp_pattern.match(line)
                        
                        if match:
                            # 1. FOUND NEW MESSAGE -> SAVE PREVIOUS ONE
                            if current_timestamp:
                                full_msg = "\n".join(current_text_buffer)
                                parsed = process_message_block(entry.name, current_timestamp, full_msg)
                                if parsed:
                                    batch_data.append(parsed)
                                    if parsed[1] != 'Template': user_msg_count += 1

                            # 2. START NEW BUFFER
                            current_timestamp = match.group(1)
                            current_text_buffer = [match.group(2).strip()] # Start with text after timestamp
                            
                        else:
                            # CONTINUATION LINE (Append to current buffer)
                            if current_timestamp:
                                current_text_buffer.append(line.strip())

                    # End of file: Save the last message in buffer
                    if current_timestamp:
                        full_msg = "\n".join(current_text_buffer)
                        parsed = process_message_block(entry.name, current_timestamp, full_msg)
                        if parsed:
                            batch_data.append(parsed)
                            if parsed[1] != 'Template': user_msg_count += 1

                # Batch Insert to DB
                if len(batch_data) >= 50000:
                    c.executemany('INSERT INTO messages VALUES (?,?,?,?,?)', batch_data)
                    conn.commit()
                    total_count += len(batch_data)
                    batch_data = []
                    print(f"Processed {total_count} msgs... (Found {user_msg_count} non-templates)")

            except Exception as e:
                print(f"⚠️ Error reading {entry.name}: {e}")

# Final Insert
if batch_data:
    c.executemany('INSERT INTO messages VALUES (?,?,?,?,?)', batch_data)
    conn.commit()

# --- INDEXING ---
print("⏳ Building Indexes...")
c.execute("CREATE INDEX IF NOT EXISTS idx_filename ON messages(filename);")
c.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON messages(timestamp);")
c.execute("CREATE INDEX IF NOT EXISTS idx_sender ON messages(sender);")
conn.commit()
conn.close()

print(f"✅ DONE! Total Messages: {total_count + len(batch_data)}")
print(f"✅ User/Human Messages Found: {user_msg_count}")