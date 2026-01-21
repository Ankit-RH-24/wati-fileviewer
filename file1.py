import os
import sqlite3
import json
import re
from pathlib import Path
from datetime import datetime

# Configuration
FOLDER_PATH = "/Users/ankit-rh/Desktop/113310_Default_chats (1)"
DB_PATH = "wati_chat_logs.db"

# Validate folder exists
if not os.path.exists(FOLDER_PATH):
    raise FileNotFoundError(f"Folder not found: {FOLDER_PATH}")

# Setup Database
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS messages (
        filename TEXT,
        sender TEXT,
        message_body TEXT,
        timestamp TEXT,
        status TEXT
    )
''')

def parse_wati_log(content, filename):
    """
    Parse WATI log format:
    [MM/DD/YYYY HH:MM:SS] Template "message content" was sent.
    """
    messages = []
    
    # Pattern to match: [timestamp] Template "message" was sent.
    # Using non-greedy match to handle multi-line messages
    pattern = r'\[(\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2})\]\s+Template\s+"(.*?)"\s+was sent\.'
    
    matches = re.finditer(pattern, content, re.DOTALL)
    
    for match in matches:
        timestamp_str = match.group(1)
        message_body = match.group(2).strip()
        
        # Convert timestamp to ISO format for better sorting
        try:
            dt = datetime.strptime(timestamp_str, "%m/%d/%Y %H:%M:%S")
            timestamp_iso = dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            timestamp_iso = timestamp_str  # Keep original if parsing fails
        
        messages.append({
            'filename': filename,
            'sender': 'Template',  # All messages are templates
            'message_body': message_body,
            'timestamp': timestamp_iso,
            'status': 'sent'
        })
    
    return messages

# Use os.scandir for better performance on large folders
batch_data = []
count = 0
errors = []

print(f"Starting ingestion from: {FOLDER_PATH}")

with os.scandir(FOLDER_PATH) as entries:
    for entry in entries:
        if entry.name.endswith(".txt") and entry.is_file():
            try:
                with open(entry.path, "r", encoding="utf-8") as f:
                    content = f.read()
                    
                    # Parse WATI log format
                    parsed_messages = parse_wati_log(content, entry.name)
                    
                    # Add each parsed message to batch
                    for msg in parsed_messages:
                        batch_data.append((
                            msg['filename'],
                            msg['sender'],
                            msg['message_body'],
                            msg['timestamp'],
                            msg['status']
                        ))
                    
                    count += len(parsed_messages)

                # Batch insert every 10,000 records to improve performance
                if len(batch_data) >= 10000:
                    cursor.executemany('INSERT INTO messages VALUES (?,?,?,?,?)', batch_data)
                    conn.commit()
                    batch_data = []
                    print(f"Processed {count} messages from {entry.name}...")
            
            except Exception as e:
                error_msg = f"Error reading {entry.name}: {e}"
                errors.append(error_msg)
                print(error_msg)

# Insert remaining records
if batch_data:
    cursor.executemany('INSERT INTO messages VALUES (?,?,?,?,?)', batch_data)
    conn.commit()
    count += len(batch_data)

print(f"\nDone! Processed {count} messages total from all files.")
if errors:
    print(f"Encountered {len(errors)} errors (see above for details).")
print(f"Database saved to: {DB_PATH}")
conn.close()