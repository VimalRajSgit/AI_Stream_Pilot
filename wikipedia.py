import json

import requests  # Ensure requests is imported
from sseclient import SSEClient as EventSource

url = "https://stream.wikimedia.org/v2/stream/recentchange"

# Wikimedia requires a descriptive User-Agent header
headers = {"User-Agent": "MyProjectBot/1.0 (contact: vimalraj@gmail.com)"}

# Pass the headers into the EventSource
# Note: sseclient uses 'requests' under the hood to handle the connection
for event in EventSource(url, headers=headers):
    if event.event == "message":
        try:
            change = json.loads(event.data)
            # Example: Print only edits from English Wikipedia
            if change.get("server_name") == "en.wikipedia.org":
                print(f"Edit on {change['title']} by {change['user']}")
        except ValueError:
            pass
