from googleapiclient.discovery import build

api_key = "xxxxxxxxxxx"
handle_name = "edurekaIN"

youtube = build('youtube', 'v3', developerKey=api_key)

request = youtube.search().list(
    part="snippet",
    q=handle_name,
    type="channel",
    maxResults=1
)
response = request.execute()

if 'items' in response and len(response['items']) > 0:
    channel = response['items'][0]
    print("Channel ID:", channel['snippet']['channelId'])
    print("Channel Title:", channel['snippet']['title'])
else:
    print("Channel not found.")

    id='UCkw4JCwteGrDHIsyIIKo4tQ'
