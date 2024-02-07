#library 
import os
import googleapiclient.discovery
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
import streamlit as st
import psycopg2
import pandas as pd
import re
load_dotenv()
from pymongo import MongoClient

# mongoDB connection
youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=os.getenv("API_KEY"))
client = MongoClient("mongodb+srv://mowli:mowlidata@cluster0.eqxrdue.mongodb.net/?retryWrites=true&w=majority")
db = client['youtube_database']

# Streamlit UI
st.set_page_config(layout="wide")
st.title("YouTube Data Analysis")
st.subheader("Enter a YouTube channel ID and get some insights")

# Get channel ID from user
channel_id = st.text_input("Channel ID")
if st.button("Store Data"):

    # '''------------ Channel data Info ------------------'''
    def get_channel_info(channel_id):
        try:
            request = youtube.channels().list(
                part="snippet,contentDetails,statistics",
                id=channel_id
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

    # '''------------ playlist data Info ------------------'''

    def get_playlist_info(channel_id):
        try:
            all_data = []
            next_page_token = None
            while True:
                request = youtube.playlists().list(
                    part="snippet,contentDetails",
                    channelId=channel_id,
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
            return all_data
        except HttpError as e:
            print("An HTTP error occurred:", e)
    playlist_data = get_playlist_info(channel_id)

    # '''------------ vedio id data Info ------------------'''

    def get_video_ids(playlist_data):
        try :
            video_ids = []
            for i in range(len(playlist_data)):
                playlist_id = playlist_data[i]['PlaylistId']
                request = youtube.playlistItems().list(
                    part="contentDetails",
                    playlistId=playlist_id,
                    maxResults=50
                )
                response = request.execute()

                for item in response.get('items', []):
                    video_ids.append(item['contentDetails']['videoId'])

                while 'nextPageToken' in response:
                    next_page_token = response['nextPageToken']
                    request = youtube.playlistItems().list(
                        part="contentDetails",
                        playlistId=playlist_id,
                        maxResults=50,
                        pageToken=next_page_token
                    )
                    response = request.execute()

                    for item in response.get('items', []):
                        video_ids.append(item['contentDetails']['videoId'])

            return video_ids
        except HttpError as e:
            print("An HTTP error occurred:", e)
    vedio_id_data = get_video_ids(playlist_data)

    def get_video_info(vedio_id_data):
        try:
            video_data = []
            for video_id in vedio_id_data:
                request = youtube.videos().list(
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
            return video_data
        except HttpError as e:
            print("An HTTP error occurred:", e)

    # '''------------ playlist data Info ------------------'''

    def get_comment_info(vedio_id_data):
        try:
            comment_information = []
            for video_id in vedio_id_data:
                try:
                    request = youtube.commentThreads().list(
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

    # '''--------------------Data------------------------'''
    def channel_details(channel_id):
        chennal_information = get_channel_info(channel_id)
        playListID = playlist_data
        VedioInfo = get_video_info(vedio_id_data)
        CommentInfo = get_comment_info(vedio_id_data)

        coll1 = db["channel_details"]
        coll1.insert_one({"channel_information":chennal_information,"playlist_information":playListID,"video_information":VedioInfo,
                        "comment_information":CommentInfo})
    
        return "upload completed successfully"
    st.write(channel_details(channel_id))
    
# '''-------------------------Migrate to PSQL(Channel data)------------------------------'''
def channels_table():
    mydb = psycopg2.connect(host="localhost",
                user="mowli",
                password="Mowli@27",
                database= "mowli",
                port = "5432"
                )
    cursor = mydb.cursor()

    drop_query = "DROP TABLE IF EXISTS channels"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''
            CREATE TABLE IF NOT EXISTS channels (
                Channel_Id VARCHAR(80) PRIMARY KEY,
                Channel_Name VARCHAR(100),
                Subscription_Count INTEGER,
                Channel_Views BIGINT,
                Total_Videos INTEGER,
                Playlist_Id VARCHAR(50),
                Channel_Description TEXT,
                Published_At TIMESTAMP
            )'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        return "Channels Table alredy created"
    
    ch_list = []
    db = client["youtube_database"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])
    df = pd.DataFrame(ch_list)
    for index, row in df.iterrows():
        channel_info = row[0]  
        insert_query = '''INSERT INTO channels (
            Channel_Id, Channel_Name, Subscription_Count, Channel_Views,
            Total_Videos, Playlist_Id, Channel_Description, Published_At
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s
        )'''

        values = (
            channel_info.get('Channel_Id', ''),
            channel_info.get('Channel_Name', ''),
            channel_info.get('Subscription_Count', ''),
            channel_info.get('Channel_Views', ''),
            channel_info.get('Total_videos', ''),
            channel_info.get('Playlist_Id', ''),
            channel_info.get('Channel_Description', ''),
            channel_info.get('Published_At', '')
        )
        try:                     
            cursor.execute(insert_query,values)
            mydb.commit() 

        except:
            st.write("Channels values are already inserted")
        return "Channel Information is Inserted Successfully"
    
    # '''-------------------------Migrate to PSQL(Playlist data)------------------------------'''

def playlists_table():
    mydb = psycopg2.connect(host="localhost",
            user="mowli",
            password="Mowli@27",
            database= "mowli",
            port = "5432"
            )
    cursor = mydb.cursor()

    drop_query = "DROP TABLE IF EXISTS playlists"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists playlists(PlaylistId varchar(100) primary key,
                        Title varchar(80), 
                        ChannelId varchar(100), 
                        ChannelName varchar(100),
                        PublishedAt timestamp,
                        VideoCount int
                        )'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        return "Playlists Table alredy created" 


    db = client["youtube_database"]
    coll1 =db["channel_details"]
    pl_list = []
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
                pl_list.append(pl_data["playlist_information"][i])
    df = pd.DataFrame(pl_list)
    
    for index,row in df.iterrows():
        insert_query = '''INSERT into playlists(PlaylistId,
                                                    Title,
                                                    ChannelId,
                                                    ChannelName,
                                                    PublishedAt,
                                                    VideoCount)
                                        VALUES(%s,%s,%s,%s,%s,%s)'''            
        values =(
                row['PlaylistId'],
                row['Title'],
                row['ChannelId'],
                row['ChannelName'],
                row['PublishedAt'],
                row['VideoCount'])
                
        try:                     
            cursor.execute(insert_query,values)
            mydb.commit()    
        except:
            return "Playlists values are already inserted"
        return "playlist information updated Successfully"

# '''-------------------------Migrate to PSQL(Vedios data)------------------------------'''
    
def format_duration(duration_str):
    try:
        if duration_str is not None:
            parts = re.findall(r'\d+', duration_str)
            parts = [part.zfill(2) for part in parts]
            return '00:' + ':'.join(parts)
        else:
            return '00:00:00'
    except AttributeError:
        print("Invalid duration format:", duration_str)
        return None
def videos_table():

    mydb = psycopg2.connect(host="localhost",
            user="mowli",
            password="Mowli@27",
            database= "mowli",
            port = "5432"
            )
    cursor = mydb.cursor()

    drop_query = "DROP TABLE IF EXISTS videos"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''CREATE TABLE IF NOT EXISTS videos(
                Video_Id VARCHAR(50) PRIMARY KEY,
                Title VARCHAR(150),
                Description TEXT,
                Published_Date TIMESTAMP,
                Channel_Name VARCHAR(150),
                Thumbnail VARCHAR(225),
                Channel_Id VARCHAR(100),
                Duration interval,
                Views BIGINT, 
                Likes BIGINT,
                Comments INT,
                Favorite_Count INT, 
                Definition VARCHAR(10), 
                Caption_Status VARCHAR(50) 
                )'''
                        
        cursor.execute(create_query)             
        mydb.commit()
    except:
        return "Videos Table alrady created"

    vi_list = []
    db = client["youtube_database"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = pd.DataFrame(vi_list)
        
    
    for index, row in df2.iterrows():
        insert_query = '''
                    INSERT INTO videos (
                        Video_Id,
                        Title,
                        Description,
                        Published_Date,
                        Channel_Name,
                        Thumbnail,
                        Channel_Id,
                        Duration,
                        Views, 
                        Likes,
                        Comments,
                        Favorite_Count, 
                        Definition, 
                        Caption_Status 
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (Video_Id) DO NOTHING
                '''
        values = (
                row['Video_Id'],
                row['Title'],
                row['Description'],
                row['Published_Date'],
                row['Channel_Name'],
                row['Thumbnail'],
                row['Channel_Id'],
                format_duration(row['Duration']),
                row['Views'],
                row['Likes'],
                row['Comments'],
                row['Favorite_Count'],
                row['Definition'],
                row['Caption_Status']
            )
                                
        try:    
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            continue
    return "videos values inserted in the table"

# '''-------------------------Migrate to PSQL(Comments data)------------------------------'''

def comments_table():
    
    mydb = psycopg2.connect(host="localhost",
            user="mowli",
            password="Mowli@27",
            database= "mowli",
            port = "5432"
            )
    cursor = mydb.cursor()

    drop_query = "DROP TABLE IF EXISTS comments"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''CREATE TABLE if not exists comments(Comment_Id varchar(100) primary key,
                       Video_Id varchar(80),
                       Comment_Text text, 
                       Comment_Author varchar(150),
                       Comment_Published timestamp)'''
        cursor.execute(create_query)
        mydb.commit()
        
    except:
        return "Commentsp Table already created"

    com_list = []
    db = client["youtube_database"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
        if "comment_information" in com_data and com_data["comment_information"] is not None:
            for i in range(len(com_data["comment_information"])):
                com_list.append(com_data["comment_information"][i])
    df3 = pd.DataFrame(com_list)


    for index, row in df3.iterrows():
        insert_query = '''
            INSERT INTO comments (Comment_Id,
                                    Video_Id ,
                                    Comment_Text,
                                    Comment_Author,
                                    Comment_Published)
            VALUES (%s, %s, %s, %s, %s)

        '''
        values = (
            row['Comment_Id'],
            row['Video_Id'],
            row['Comment_Text'],
            row['Comment_Author'],
            row['Comment_Published']
        )
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            continue
    return "Comment information is Updated Successfully"
if st.button("Migrate to SQL"):
    st.success(channels_table())
    st.success(playlists_table())
    st.success(videos_table())
    st.success(comments_table())

# '''-------------------------QA------------------------------'''
    
if 'qa_session_active' not in st.session_state:
    st.session_state.qa_session_active = False

with st.form(key='qa_form'):
    st.write('Click the button to start the QA Session:')
    qa_button_clicked = st.form_submit_button('QA Session')

    if qa_button_clicked:
        st.session_state.qa_session_active = not st.session_state.qa_session_active

    if st.session_state.qa_session_active:
        mydb = psycopg2.connect(host="localhost",
                                user="mowli",
                                password="Mowli@27",
                                database="mowli",
                                port="5432"
                                )
        cursor = mydb.cursor()

        question_key = "selectbox" 

        question = st.selectbox(
            'Please Select Your Question',
            ('1. All the videos and the Channel Name',
            '2. Channels with most number of videos',
            '3. 10 most viewed videos',
            '4. Comments in each video',
            '5. Videos with highest likes',
            '6. likes of all videos',
            '7. views of each channel',
            '8. videos published in the year 2022',
            '9. average duration of all videos in each channel',
            '10. videos with highest number of comments'),
            key=question_key
        )

        submit_button_clicked = st.form_submit_button('Submit')

        if submit_button_clicked:
            if question == '1. All the videos and the Channel Name':
                query1 = "select Title as videos, Channel_Name as ChannelName from videos;"
                cursor.execute(query1)
                mydb.commit()
                t1=cursor.fetchall()
                st.write(pd.DataFrame(t1, columns=["Video Title","Channel Name"]))

            elif question == '2. Channels with most number of videos':
                query2 = "select Channel_Name as ChannelName,Total_Videos as NO_Videos from channels order by Total_Videos desc;"
                cursor.execute(query2)
                mydb.commit()
                t2=cursor.fetchall()
                st.write(pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]))

            elif question == '3. 10 most viewed videos':
                query3 = '''select Views as views , Channel_Name as ChannelName,Title as VideoTitle from videos 
                                    where Views is not null order by Views desc limit 10;'''
                cursor.execute(query3)
                mydb.commit()
                t3 = cursor.fetchall()
                st.write(pd.DataFrame(t3, columns = ["views","channel Name","video title"]))

            elif question == '4. Comments in each video':
                query4 = "select Comments as No_comments ,Title as VideoTitle from videos where Comments is not null;"
                cursor.execute(query4)
                mydb.commit()
                t4=cursor.fetchall()
                st.write(pd.DataFrame(t4, columns=["No Of Comments", "Video Title"]))

            elif question == '5. Videos with highest likes':
                query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos 
                                where Likes is not null order by Likes desc;'''
                cursor.execute(query5)
                mydb.commit()
                t5 = cursor.fetchall()
                st.write(pd.DataFrame(t5, columns=["video Title","channel Name","like count"]))

            elif question == '6. likes of all videos':
                query6 = '''select Likes as likeCount,Title as VideoTitle from videos;'''
                cursor.execute(query6)
                mydb.commit()
                t6 = cursor.fetchall()
                st.write(pd.DataFrame(t6, columns=["like count","video title"]))

            elif question == '7. views of each channel':
                query7 = "select Channel_Name as ChannelName, Channel_Views as Channelviews from channels;"
                cursor.execute(query7)
                mydb.commit()
                t7=cursor.fetchall()
                st.write(pd.DataFrame(t7, columns=["channel name","total views"]))

            elif question == '8. videos published in the year 2022':
                query8 = '''select Title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos 
                            where extract(year from Published_Date) = 2022;'''
                cursor.execute(query8)
                mydb.commit()
                t8=cursor.fetchall()
                st.write(pd.DataFrame(t8,columns=["Name", "Video Publised On", "ChannelName"]))

            elif question == '9. average duration of all videos in each channel':
                query9 =  "SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name;"
                cursor.execute(query9)
                mydb.commit()
                t9=cursor.fetchall()
                t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
                T9=[]
                for index, row in t9.iterrows():
                    channel_title = row['ChannelTitle']
                    average_duration = row['Average Duration']
                    average_duration_str = str(average_duration)
                    T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
                st.write(pd.DataFrame(T9))

            elif question == '10. videos with highest number of comments':
                query10 = '''select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos 
                                where Comments is not null order by Comments desc;'''
                cursor.execute(query10)
                mydb.commit()
                t10=cursor.fetchall()
                st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments']))

    form_submit_button = st.form_submit_button('Submit_QA_Session')