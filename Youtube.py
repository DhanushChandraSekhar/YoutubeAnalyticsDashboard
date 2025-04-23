from googleapiclient.discovery import build
import pandas as pd
from sqlalchemy import create_engine

# API credentials and channel info
api_key = 'xxxxxxxxx'
channel_id = 'UCkw4JCwteGrDHIsyIIKo4tQ'

# Initialize YouTube API client
youtube = build('youtube', 'v3', developerKey=api_key)

# Step 1: Get the uploads playlist ID for the channel
channel_response = youtube.channels().list(
    part='contentDetails',
    id=channel_id
).execute()

uploads_playlist_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

# Step 2: Get video IDs from the playlist
video_ids = []
next_page_token = None

while True:
    playlist_response = youtube.playlistItems().list(
        part='contentDetails',
        playlistId=uploads_playlist_id,
        maxResults=50,
        pageToken=next_page_token
    ).execute()

    for item in playlist_response['items']:
        video_ids.append(item['contentDetails']['videoId'])

    next_page_token = playlist_response.get('nextPageToken')
    if not next_page_token:
        break

# Step 3: Fetch video details in batches
video_data = []

for i in range(0, len(video_ids), 50):
    response = youtube.videos().list(
        part='snippet,statistics',
        id=','.join(video_ids[i:i+50])
    ).execute()

    for item in response['items']:
        video_data.append({
            'video_id': item['id'],
            'title': item['snippet']['title'],
            'published_at': item['snippet']['publishedAt'],
            'views': item['statistics'].get('viewCount', 0),
            'likes': item['statistics'].get('likeCount', 0),
            'comments': item['statistics'].get('commentCount', 0)
        })

# Step 4: Create DataFrame
df = pd.DataFrame(video_data)

# Step 5: Data cleanup and enrichment
df['published_at'] = pd.to_datetime(df['published_at'], utc=True)
df[['views', 'likes', 'comments']] = df[['views', 'likes', 'comments']].astype(int)
df['days_since_published'] = (pd.Timestamp.now(tz='UTC') - df['published_at']).dt.days
df['engagement_rate'] = ((df['likes'] + df['comments']) / df['views']) * 1000

# Extract upload month and convert to string
df['upload_month'] = df['published_at'].dt.to_period('M').astype(str)

# Optional: Normalize column names
df.columns = df.columns.str.lower()

# Step 6: Save to PostgreSQL database
engine = create_engine("postgresql://postgres:<password>@localhost:xxxx/youtube_db")
df.to_sql("video_data", con=engine, if_exists="replace", index=False)

print("Data uploaded successfully to PostgreSQL.")

df.to_csv("youtube_data.csv", index=False)

print("file downloaded")
