import csv
import os

# Ensure input directory exists
os.makedirs("input", exist_ok=True)

# Define songs metadata
songs_metadata = [
    [101, "Song A", "Artist 1", "Pop", "Happy"],
    [102, "Song B", "Artist 2", "Pop", "Happy"],
    [103, "Song C", "Artist 3", "Pop", "Happy"],
    [104, "Song D", "Artist 4", "Rock", "Happy"],
    [105, "Song E", "Artist 5", "Rock", "Sad"],
    [106, "Song F", "Artist 6", "Rock", "Sad"],
    [107, "Song G", "Artist 7", "Rock", "Sad"],
    [108, "Song H", "Artist 8", "Jazz", "Chill"],
    [109, "Song I", "Artist 9", "Jazz", "Chill"],
    [110, "Song J", "Artist 10", "Jazz", "Chill"]
]

# Save to CSV
with open("input/songs_metadata.csv", "w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["song_id", "title", "artist", "genre", "mood"])
    writer.writerows(songs_metadata)

print("Generated songs_metadata.csv")
