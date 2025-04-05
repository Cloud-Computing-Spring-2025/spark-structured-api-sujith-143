import csv
import os
from datetime import datetime, timedelta
import random

# Ensure input directory exists
os.makedirs("input", exist_ok=True)

# Define user and song distributions
users = [1, 2, 3, 4, 5]
songs = {
    101: "Pop", 102: "Pop", 103: "Pop",
    104: "Rock", 105: "Rock", 106: "Rock", 107: "Rock",
    108: "Jazz", 109: "Jazz", 110: "Jazz"
}
night_owl_users = {2, 4}  # Users who listen at night

# Generate listening logs
log_entries = []
current_date = datetime(2025, 3, 23, 14, 0, 0)

for user in users:
    for _ in range(random.randint(4, 6)):  # Each user listens to 4-6 songs
        song_id = random.choice(list(songs.keys()))
        
        # Assign night listening times to night owl users
        if user in night_owl_users and random.random() < 0.5:
            hour = random.choice([0, 1, 2, 3, 4])
        else:
            hour = random.randint(10, 22)
        
        timestamp = current_date.replace(hour=hour, minute=random.randint(0, 59), second=0)
        duration = random.randint(180, 260)  # Play time in seconds

        log_entries.append([user, song_id, timestamp.strftime("%Y-%m-%d %H:%M:%S"), duration])

# Save to CSV
with open("input/listening_logs.csv", "w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["user_id", "song_id", "timestamp", "duration_sec"])
    writer.writerows(log_entries)

print("Generated listening_logs.csv")
