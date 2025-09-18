# Hog memory on a database node to trigger low-memory alert and OOM killer

import time

data = []

try:
    while True:
        data.append([0] * (1024 * 1024 * 256))
        print(f"Allocated {len(data)} GB.")
        time.sleep(0.5)
except KeyboardInterrupt:
    print("Stopped.")
