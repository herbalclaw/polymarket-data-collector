import sqlite3
import time
import threading

# Simulate the collector setup
conn = sqlite3.connect('/tmp/test_rapid.db', check_same_thread=False)
conn.execute('PRAGMA journal_mode=WAL')
conn.execute('PRAGMA synchronous=NORMAL')
conn.execute('CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, ts INTEGER, data REAL)')
conn.commit()

buffer = []
buffer_lock = threading.Lock()

def flush():
    with buffer_lock:
        if buffer:
            conn.executemany('INSERT INTO test (ts, data) VALUES (?, ?)', buffer)
            conn.commit()
            buffer.clear()

start = time.time()
count = 0
while time.time() - start < 10:
    with buffer_lock:
        buffer.append((int(time.time()*1000), 0.5))
    count += 1
    if count % 20 == 0:
        flush()
    time.sleep(0.05)

flush()
print(f'Wrote {count} rows')
conn.close()
