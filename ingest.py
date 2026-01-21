import os
import sqlite3
import re
from datetime import datetime

# CONFIGURATION
FOLDER_PATH = "/Users/ankit-rh/Desktop/113310_Default_chats (1)"  # CHECK THIS PATH
DB_PATH = "wati_chat_logs.db"

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# 1. Enable WAL Mode (Prevents corruption & speeds up writes)
c.execute("PRAGMA journal_mode=WAL;")
c.execute("PRAGMA synchronous=NORMAL;")

# 2. Create Table
c.execute('''
    CREATE TABLE IF NOT EXISTS messages (
        filename TEXT,
        sender TEXT,
        message_body TEXT,
        timestamp TEXT,
        status TEXT
    )
''')

# 3. Fast Parsing Logic
def parse_wati_log(content, filename):
    messages = []
    # Regex to capture Timestamp and Message
    pattern = r'\[(\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2})\]\s+Template\s+"(.*?)"\s+was sent\.'
    matches = re.finditer(pattern, content, re.DOTALL)
    
    for match in matches:
        timestamp_str = match.group(1)
        message_body = match.group(2).strip()
        try:
            dt = datetime.strptime(timestamp_str, "%m/%d/%Y %H:%M:%S")
            timestamp_iso = dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            timestamp_iso = timestamp_str
        
        messages.append((filename, 'Template', message_body, timestamp_iso, 'sent'))
    return messages

print("🚀 Starting robust ingestion...")

batch_data = []
count = 0

# 4. Scan and Insert
with os.scandir(FOLDER_PATH) as entries:
    for entry in entries:
        if entry.name.endswith(".txt"):
            try:
                with open(entry.path, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read()
                    parsed = parse_wati_log(content, entry.name)
                    batch_data.extend(parsed)
                    count += len(parsed)

                # Insert every 50,000 records
                if len(batch_data) >= 50000:
                    c.executemany('INSERT INTO messages VALUES (?,?,?,?,?)', batch_data)
                    conn.commit()
                    batch_data = []
                    print(f"Processed {count} messages...")

            except Exception as e:
                print(f"Skipping {entry.name}: {e}")

# Insert remaining
if batch_data:
    c.executemany('INSERT INTO messages VALUES (?,?,?,?,?)', batch_data)
    conn.commit()

# 5. CREATE INDEXES NOW (So you don't need a separate script)
print("⏳ Building Indexes (This makes the viewer fast)...")
c.execute("CREATE INDEX IF NOT EXISTS idx_filename ON messages(filename);")
c.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON messages(timestamp);")
c.execute("CREATE INDEX IF NOT EXISTS idx_body ON messages(message_body);")
conn.commit()

conn.close()
print("✅ Done! Database rebuilt successfully.")