import os
import googleapiclient.discovery
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
import psycopg2
import pandas as pd
from pymongo import MongoClient
import re
load_dotenv()

class YouTubeDataAnalyzer():
    def __init__(self, channel_id):
        self.youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=os.getenv("API_KEY"))
        self.client = MongoClient("mongodb+srv://mowli:mowlidata@cluster0.eqxrdue.mongodb.net/?retryWrites=true&w=majority")
        self.db = self.client['youtube_info_data']
        self.channel_id = channel_id
        self.playlist_data = None
        self.video_ids = None
        self.video_info = None

    def get_channel_info(self):
        try:
            request = self.youtube.channels().list(
                part="snippet,contentDetails,statistics",
                id=self.channel_id
            )
            response = request.execute()
            channel_data = []
            for item in response.get("items", []):
                data = {
                    "Channel_Id": item["id"],
                    "Channel_Name": item["snippet"]["title"],
                    "Subscription_Count": item["statistics"]["subscriberCount"],
                    "Channel_Views": item["statistics"]["viewCount"],
                    "Total_videos": item["statistics"]["videoCount"],
                    "Playlist_Id": item["contentDetails"]["relatedPlaylists"]["uploads"],
                    "Channel_Description": item["snippet"]["description"],
                    "Published_At": item["snippet"]["publishedAt"]
                }
                channel_data.append(data)
            return channel_data
        except HttpError as e:
            print("An HTTP error occurred:", e)

    def get_playlist_info(self):
        try:
            all_data = []
            next_page_token = None
            while True:
                request = self.youtube.playlists().list(
                    part="snippet,contentDetails",
                    channelId=self.channel_id,
                    maxResults=50,
                    pageToken=next_page_token
                )
                response = request.execute()
                for item in response.get("items", []):
                    data = {
                        "PlaylistId": item["id"],
                        "Title": item["snippet"]["title"],
                        "ChannelId": item["snippet"]["channelId"],
                        "ChannelName": item["snippet"]["channelTitle"],
                        "PublishedAt": item["snippet"]["publishedAt"],
                        "VideoCount": item["contentDetails"]["itemCount"]
                    }
                    all_data.append(data)
                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    break
            self.playlist_data = all_data
        except HttpError as e:
            print("An HTTP error occurred:", e)

    def get_video_ids(self):
        try:
            video_ids = []
            for i in range(len(self.playlist_data)):
                playlist_id = self.playlist_data[i]['PlaylistId']
                request = self.youtube.playlistItems().list(
                    part="contentDetails",
                    playlistId=playlist_id,
                    maxResults=50
                )
                response = request.execute()

                for item in response.get('items', []):
                    video_ids.append(item['contentDetails']['videoId'])

                while 'nextPageToken' in response:
                    next_page_token = response['nextPageToken']
                    request = self.youtube.playlistItems().list(
                        part="contentDetails",
                        playlistId=playlist_id,
                        maxResults=50,
                        pageToken=next_page_token
                    )
                    response = request.execute()

                    for item in response.get('items', []):
                        video_ids.append(item['contentDetails']['videoId'])

            self.video_ids = video_ids
        except HttpError as e:
            print("An HTTP error occurred:", e)

    def get_video_info(self):
        try:
            video_data = []
            for video_id in self.video_ids:
                request = self.youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=video_id
                )
                response = request.execute()
                for item in response.get("items", []):
                    data = {
                        "Video_Id": item["id"],
                        "Title": item["snippet"]["title"],
                        "Description": item["snippet"]["description"],
                        "Published_Date": item["snippet"]["publishedAt"],
                        "Channel_Name": item["snippet"]["channelTitle"],
                        "Thumbnail": item["snippet"]["thumbnails"]["default"]["url"],
                        "Channel_Id": item["snippet"]["channelId"],
                        "Duration" : item['contentDetails']['duration'],
                        "Views": item["statistics"]["viewCount"],
                        "Likes": item["statistics"].get("likeCount"),
                        "Comments": item["statistics"].get("commentCount"),
                        "Favorite_Count": item["statistics"]["favoriteCount"],
                        "Definition": item["contentDetails"]["definition"],
                        "Caption_Status": item["contentDetails"]["caption"]
                    }
                    video_data.append(data)
            self.video_info = video_data
        except HttpError as e:
            print("An HTTP error occurred:", e)

    def get_comment_info(self):
        try:
            comment_information = []
            for video_id in self.video_ids:
                try:
                    request = self.youtube.commentThreads().list(
                        part="snippet",
                        videoId=video_id,
                        maxResults=50
                    )
                    response = request.execute()
                    for item in response.get("items", []):
                        comment_data = {
                            "Comment_Id": item["snippet"]["topLevelComment"]["id"],
                            "Video_Id": item["snippet"]["videoId"],
                            "Comment_Text": item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                            "Comment_Author": item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                            "Comment_Published": item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                        }
                        comment_information.append(comment_data)
                except HttpError as comment_error:

                    if comment_error.resp.status == 404:
                        print(f"Video not found for videoId: {video_id}")
                    else:
                        print(f"An HTTP error occurred for videoId {video_id}: {comment_error}")

            return comment_information
        except HttpError as e:
            print("An HTTP error occurred:", e)

    def store_data(self):
        channel_information = self.get_channel_info()
        self.get_playlist_info()
        self.get_video_ids()
        self.get_video_info()
        comment_info = self.get_comment_info()

        coll1 = self.db["channel_details"]
        coll1.insert_one({"channel_information": channel_information, "playlist_information": self.playlist_data,
                         "video_information": self.video_info, "comment_information": comment_info})

        return "Upload completed successfully"
class SQLMigrator:
    def __init__(self):
        self.pg_connection = psycopg2.connect(
            host="localhost",
            user="mowli",
            password="Mowli@27",
            database="youtube_data_information",
            port="5432"
        )
        self.pg_cursor = self.pg_connection.cursor()
        self.client = MongoClient("mongodb+srv://mowli:mowlidata@cluster0.eqxrdue.mongodb.net/?retryWrites=true&w=majority")
        self.db = self.client['youtube_info_data']

    def channels_table(self, channel_names):
        try:

            self.pg_cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'channels')")
            if not self.pg_cursor.fetchone()[0]:
                self.pg_cursor.execute('''
                    CREATE TABLE channels (
                        Channel_Id VARCHAR(255) PRIMARY KEY,
                        Channel_Name VARCHAR(255) NOT NULL,
                        Subscription_Count INTEGER,
                        Channel_Views INTEGER,
                        Total_Videos INTEGER,
                        Playlist_Id VARCHAR(255),
                        Channel_Description TEXT,
                        Published_At TIMESTAMP
                    );
                ''')
                self.pg_connection.commit()

         
            if channel_names == "All Channels":
                documents = self.db.channel_details.find({})
            else:
                documents = self.db.channel_details.find({"channel_information.Channel_Name": {"$in": channel_names}})

            for doc in documents:
                channel_info = doc["channel_information"][0]
                insert_query = '''
                    INSERT INTO channels (Channel_Id, Channel_Name, Subscription_Count, Channel_Views, Total_Videos, Playlist_Id, Channel_Description, Published_At)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (Channel_Id) DO NOTHING;
                '''
                self.pg_cursor.execute(insert_query, (
                    channel_info.get('Channel_Id'),
                    channel_info.get('Channel_Name'),
                    channel_info.get('Subscription_Count', 0),
                    channel_info.get('Channel_Views', 0),
                    channel_info.get('Total_videos', 0),
                    channel_info.get('Playlist_Id', ''),
                    channel_info.get('Channel_Description', ''),
                    channel_info.get('Published_At', None)
                ))
            self.pg_connection.commit()

            return "Channels information inserted successfully."
        except Exception as e:
            return f"An error occurred: {e}"

    def playlists_table(self, channel_name):
        try:
            coll1 = self.db["channel_details"]
            mydb = psycopg2.connect(
                host="localhost",
                user="mowli",
                password="Mowli@27",
                database="youtube_data_information",
                port="5432"
            )
            cursor = mydb.cursor()

           
            cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'playlists')")
            table_exists = cursor.fetchone()[0]

         
            if not table_exists:
                create_query = '''CREATE TABLE IF NOT EXISTS playlists(
                    PlaylistId VARCHAR(100) PRIMARY KEY,
                    Title VARCHAR(80),
                    ChannelId VARCHAR(100),
                    ChannelName VARCHAR(100),
                    PublishedAt TIMESTAMP,
                    VideoCount INT
                )'''
                cursor.execute(create_query)
                mydb.commit()

       
            if channel_name == "All Channels":
                documents = coll1.find({})
            else:
                documents = coll1.find({"channel_information.Channel_Name": channel_name}, {"_id": 0, "playlist_information": 1})
            
            pl_list = []
            
          
            for doc in documents:
                for pl_info in doc.get("playlist_information", []):
                    pl_list.append(pl_info)

            df = pd.DataFrame(pl_list)

        
            for index, row in df.iterrows():
                insert_query = '''
                    INSERT INTO playlists (PlaylistId, Title, ChannelId, ChannelName, PublishedAt, VideoCount)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (PlaylistId) DO UPDATE SET
                    Title = EXCLUDED.Title,
                    ChannelId = EXCLUDED.ChannelId,
                    ChannelName = EXCLUDED.ChannelName,
                    PublishedAt = EXCLUDED.PublishedAt,
                    VideoCount = EXCLUDED.VideoCount;
                '''
                values = (
                    row['PlaylistId'],
                    row['Title'],
                    row['ChannelId'],
                    row['ChannelName'],
                    row['PublishedAt'],
                    row['VideoCount']
                )

                cursor.execute(insert_query, values)
                mydb.commit()

            return "Playlist information updated successfully"

        except Exception as e:
            return f"An error occurred: {e}"

    def format_duration(self, duration_str):
        try:
            if duration_str is not None:
                parts = re.findall(r'\d+', duration_str)
                parts = [part.zfill(2) for part in parts]
                if len(parts) == 3:
                    return ':'.join(parts)
                elif len(parts) == 2:
                    return '00:' + ':'.join(parts)
                elif len(parts) == 1:
                    return '00:00:' + parts[0]
                else:
                    return '00:00:00'
            else:
                return '00:00:00'
        except AttributeError as e:
            print(f"Invalid duration format: {duration_str}, Error: {e}")
            return None

    def videos_table(self, channel_name):
        try:
            mydb = psycopg2.connect(
                host="localhost",
                user="mowli",
                password="Mowli@27",
                database="youtube_data_information",
                port="5432"
            )
            cursor = mydb.cursor()
            cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'videos')")
            table_exists = cursor.fetchone()[0]

            if not table_exists:
                create_query = '''CREATE TABLE IF NOT EXISTS videos(
                    Video_Id VARCHAR(50) PRIMARY KEY,
                    Title VARCHAR(150),
                    Description TEXT,
                    Published_Date TIMESTAMP,
                    Channel_Name VARCHAR(150),
                    Thumbnail VARCHAR(225),
                    Channel_Id VARCHAR(100),
                    Duration INTERVAL,
                    Views BIGINT, 
                    Likes BIGINT,
                    Comments INT,
                    Favorite_Count INT, 
                    Definition VARCHAR(10), 
                    Caption_Status VARCHAR(50)
                )'''
                cursor.execute(create_query)
                mydb.commit()

            if channel_name == "All Channels":
                documents = self.db.channel_details.find({})
            else:
                documents = self.db.channel_details.find({"channel_information.Channel_Name": channel_name}, {"_id": 0, "video_information": 1})

            vi_list = []

            for doc in documents:
                for vi_info in doc.get("video_information", []):
                    vi_list.append(vi_info)

            df = pd.DataFrame(vi_list)

            for index, row in df.iterrows():
                insert_query = '''
                    INSERT INTO videos (
                        Video_Id, Title, Description, Published_Date, Channel_Name,
                        Thumbnail, Channel_Id, Duration, Views, Likes, Comments,
                        Favorite_Count, Definition, Caption_Status
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (Video_Id) DO UPDATE SET
                    Title = EXCLUDED.Title,
                    Description = EXCLUDED.Description,
                    Published_Date = EXCLUDED.Published_Date,
                    Channel_Name = EXCLUDED.Channel_Name,
                    Thumbnail = EXCLUDED.Thumbnail,
                    Channel_Id = EXCLUDED.Channel_Id,
                    Duration = EXCLUDED.Duration,
                    Views = EXCLUDED.Views,
                    Likes = EXCLUDED.Likes,
                    Comments = EXCLUDED.Comments,
                    Favorite_Count = EXCLUDED.Favorite_Count,
                    Definition = EXCLUDED.Definition,
                    Caption_Status = EXCLUDED.Caption_Status;
                '''
                values = (
                    row['Video_Id'], row['Title'], row['Description'], row['Published_Date'], row['Channel_Name'],
                    row['Thumbnail'], row['Channel_Id'], self.format_duration(row['Duration']), row['Views'], row['Likes'],
                    row['Comments'], row['Favorite_Count'], row['Definition'], row['Caption_Status']
                )

                cursor.execute(insert_query, values)
                mydb.commit()

            return "Video information inserted/updated successfully"

        except Exception as e:
            return f"An error occurred: {e}"
    def comments_table(self, channel_name):
        try:
            mydb = psycopg2.connect(
                host="localhost",
                user="mowli",
                password="Mowli@27",
                database="youtube_data_information",
                port="5432"
            )
            cursor = mydb.cursor()

            cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'comments')")
            table_exists = cursor.fetchone()[0]

            if not table_exists:
                create_query = '''CREATE TABLE IF NOT EXISTS comments(
                    Comment_Id VARCHAR(100) PRIMARY KEY,
                    Video_Id VARCHAR(80),
                    Comment_Text TEXT, 
                    Comment_Author VARCHAR(150),
                    Comment_Published TIMESTAMP
                )'''
                cursor.execute(create_query)
                mydb.commit()

            if channel_name == "All Channels":
                documents = self.db.channel_details.find({})
            else:
                documents = self.db.channel_details.find({"channel_information.Channel_Name": channel_name}, {"_id": 0, "comment_information": 1})
            
            com_list = []

            for doc in documents:
                if "comment_information" in doc and doc["comment_information"] is not None:
                    for com_info in doc["comment_information"]:
                        com_list.append(com_info)

            df = pd.DataFrame(com_list)

            for index, row in df.iterrows():
                insert_query = '''
                    INSERT INTO comments (Comment_Id, Video_Id, Comment_Text, Comment_Author, Comment_Published)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (Comment_Id) DO UPDATE SET
                    (Video_Id, Comment_Text, Comment_Author, Comment_Published) = 
                    (EXCLUDED.Video_Id, EXCLUDED.Comment_Text, EXCLUDED.Comment_Author, EXCLUDED.Comment_Published);
                '''
                values = (
                    row['Comment_Id'],
                    row['Video_Id'],
                    row['Comment_Text'],
                    row['Comment_Author'],
                    row['Comment_Published']
                )

                cursor.execute(insert_query, values)
                mydb.commit()

            return "Comment information is inserted successfully"
        except Exception as e:
            return f"An error occurred: {e}"
