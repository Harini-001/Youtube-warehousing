import streamlit as st

import pandas as pd

import numpy as np

from googleapiclient.discovery import build

from googleapiclient.errors import HttpError

import re

import json



st.title('📺 YouTube Data Harvesting and Warehousing')

st.markdown('### Built with Streamlit')



import pandas as pd
import re
from sqlalchemy import create_engine
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import sqlite3

# Connect to the SQLite database 'db1'
conn = sqlite3.connect('db1.db')

# Create a cursor object to execute SQL commands
cursor = conn.cursor()

# Drop the 'youtube_data' table if it exists
cursor.execute('DROP TABLE IF EXISTS youtube_data')

# Commit the changes to the database
conn.commit()

# Close the connection
conn.close()

#Used Google API key to fetch the data from youtube
def Api_connector():
    apikey="AIzaSyC50U1dJr0jYojCHsLCRXUIeDeceWSDsxw"
    api_service_name = "youtube"
    api_version = "v3"
    youtube=build(api_service_name,api_version,developerKey=apikey)
    return youtube
youtube=Api_connector()
import sqlite3
conn = sqlite3.connect("db1.db")
cursor = conn.cursor()

# Make sure the table exists before any other operation
cursor.execute('''
    CREATE TABLE IF NOT EXISTS videos (
        Video_Id TEXT PRIMARY KEY,
        Video_title TEXT,
        Video_Description TEXT,
        channel_id TEXT,
        video_tags TEXT,
        Video_pubdate TEXT,
        Video_viewcount INTEGER,
        Video_likecount INTEGER,
        Video_favoritecount INTEGER,
        Video_commentcount INTEGER,
        Video_duration TEXT,
        Video_thumbnails TEXT,
        Video_caption TEXT
    )
''')
conn.commit()
conn.close()
if st.button("🔍 Check Existing Tables"):
    conn = sqlite3.connect("db1.db")
    tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn)
    conn.close()
    st.dataframe(tables)

#created the function to fetch the channel deatils
def channel_info(channel_id):
    request = youtube.channels().list(
                    part="snippet,contentDetails,statistics",
                    id=channel_id
    )
    response = request.execute()
    data = {
                        "Channel_Name": response["items"][0]["snippet"]["title"],
                        "Channel_Id": response["items"][0]["id"],
                        "Channel_Des": response["items"][0]["snippet"]["description"],
                        "Channel_playid": response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"],
                        "channel_viewcount": response["items"][0]["statistics"]["viewCount"],
                        "channel_subcount": response["items"][0]["statistics"]["subscriberCount"]
                         }
    df=pd.DataFrame(data,index=[0])
    return df

import pandas as pd
import sqlite3

def eachchanneldetails(channel_ids):
    # Connect to the SQLite database 'youtube_data.db'
    conn = sqlite3.connect('db1.db')

    # Create a cursor object to execute SQL commands
    cursor = conn.cursor()

    # Create the 'channels' table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS channels (
            channel_name TEXT,
            channel_id TEXT,
            channel_des TEXT,
            channel_playid TEXT,
            channel_viewcount INTEGER,
            channel_subcount INTEGER
        )
    ''')

    # Commit the table creation
    conn.commit()

    for channel_id in channel_ids:
        df = channel_info(channel_id)  # Assuming this function fetches channel data
        df.to_sql('channels', conn, if_exists='append', index=False)

    # Close the connection
    conn.close()

channel_ids = ['UCVlNQ5Olu3Uiv5FL8e-yEmQ']
eachchanneldetails(channel_ids)

#Fetch the video ids from playlist id using Channel id
def playlist_videos_id(channel_ids):
    all_video_ids=[]
    for channels_id in channel_ids:
        videos_ids=[]
        response = youtube.channels().list(
        part="contentDetails",
        id=channels_id
        ).execute()
        playlist_Id=response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

        nextPageToken=None

        while True:
            response2 = youtube.playlistItems().list(
               part="snippet",
               playlistId=playlist_Id,maxResults=50,
               pageToken=nextPageToken).execute()
            for i in range(len(response2["items"])):
               videos_ids.append(response2["items"][i]["snippet"]["resourceId"]["videoId"])
            nextPageToken=response2.get("nextPageToken")

            if nextPageToken is None:
               break
        all_video_ids.extend(videos_ids)
    return all_video_ids

#Function for converting the hours to Seconds
def iso8601_duration_to_seconds(duration):
    match = re.match(r'^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$', duration)
    if not match:
        return None

    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0

    total_seconds = (hours * 3600) + (minutes * 60) + seconds
    return total_seconds

#Function to fetch the video datas from video id
from googleapiclient.errors import HttpError

def videos_data(video_ids):
    st.subheader("Fetching Video Details")
    
    try:
        all_video_stats = []

        for video_id in video_ids:
            st.write(f"▶️ Fetching for video ID: `{video_id}`")
            request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=video_id
            )
            response = request.execute()

            if response["items"]:
                video = response["items"][0]
                video_info = {
                    "Video ID": video_id,
                    "Title": video["snippet"]["title"],
                    "Views": video["statistics"].get("viewCount", 0),
                    "Likes": video["statistics"].get("likeCount", 0),
                    "Comments": video["statistics"].get("commentCount", 0),
                    "Published At": video["snippet"]["publishedAt"],
                }
                all_video_stats.append(video_info)

        return pd.DataFrame(all_video_stats)

    except HttpError as e:
        st.error(f"❌ YouTube API quota error or bad ID: {e}")
        return pd.DataFrame()

import pandas as pd
import sqlite3

def insert_videos_into_sqlite(df1):
    conn = sqlite3.connect('db1.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS videos (
            Video_Id TEXT,
            Video_title TEXT,
            Video_Description TEXT,
            channel_id TEXT,
            video_tags TEXT,
            Video_pubdate TEXT,
            Video_viewcount INTEGER,
            Video_likecount INTEGER,
            Video_favoritecount INTEGER,
            Video_commentcount INTEGER,
            Video_duration TEXT,
            Video_thumbnails TEXT,
            Video_caption TEXT
        )
    ''')
    conn.commit()
    df1.to_sql('videos', conn, if_exists='append', index=False)
    conn.close()
    st.success("✅ Data inserted into 'videos' table!")


# ✅ Put this inside a button!
if st.button("Fetch and Store Videos"):
    try:
        request = youtube.channels().list(
            part="contentDetails",
            id=channel_id
        )
        response = request.execute()
        playlist_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        video_ids = get_video_ids_from_playlist(playlist_id)

        df1 = videos_data(video_ids)

        if not df1.empty:
            st.dataframe(df1)
            insert_videos_into_sqlite(df1)
        else:
            st.warning("⚠️ No videos found for this channel.")
    except Exception as e:
        st.error(f"❌ Error: {e}")

from googleapiclient.errors import HttpError
import pandas as pd

def comments_inf(video_ids):
    commentdata = []
    try:
        for video_id in video_ids:
            nextpagetoken = None
            while True:
                try:
                    request = youtube.commentThreads().list(
                        part="snippet",
                        videoId=video_id,
                        maxResults=50,
                        pageToken=nextpagetoken
                    )
                    response = request.execute()

                    for item in response["items"]:
                        comment = {
                            "Comment_Id": item["snippet"]["topLevelComment"]["id"],
                            "Comment_Text": item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                            "Comment_Authorname": item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                            "published_date": item["snippet"]["topLevelComment"]["snippet"]["publishedAt"],
                            "video_id": item["snippet"]["topLevelComment"]["snippet"]["videoId"],
                            "channel_id": channel_id  # Passed externally
                        }
                        commentdata.append(comment)

                    nextpagetoken = response.get("nextPageToken")
                    if not nextpagetoken:
                        break

                except HttpError as e:
                    if e.resp.status == 403:
                        st.warning(f"⚠️ Comments are disabled for video ID: {video_id}")
                        break
                    else:
                        raise
    except Exception as e:
        st.error(f"❌ Error while fetching comments: {e}")

    return pd.DataFrame(commentdata)


import pandas as pd
import sqlite3

def insert_comments_into_sqlite(df2):
    conn = sqlite3.connect('db1.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS comments (
            comment_id TEXT,
            Comment_Text TEXT,
            comment_authorname TEXT,
            published_date TEXT,
            video_id TEXT,
            channel_id TEXT
        )
    ''')
    conn.commit()
    df2.to_sql('comments', conn, if_exists='append', index=False)
    conn.close()
if st.button("Fetch & Insert Comments"):
    try:
        request = youtube.channels().list(
            part="contentDetails",
            id=channel_id
        )
        response = request.execute()
        playlist_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

        video_ids = get_video_ids_from_playlist(playlist_id)
        df2 = comments_inf(video_ids)

        if not df2.empty:
            insert_comments_into_sqlite(df2)
            st.dataframe(df2.head())
            st.success("✅ Comments inserted into SQLite DB.")
        else:
            st.warning("⚠️ No comments fetched.")

    except Exception as e:
        st.error(f"❌ Error: {e}")

if st.button("Show Videos Table"):
    try:
        conn = sqlite3.connect("db1.db")
        df = pd.read_sql("SELECT * FROM videos", conn)
        conn.close()
        if df.empty:
            st.warning("⚠️ Videos table exists, but it's empty.")
        else:
            st.dataframe(df)
    except Exception as e:
        st.error(f"❌ Error accessing videos table: {e}")


import sqlite3
import pandas as pd

def fetch_video_and_channel_names():
    # Connect to the SQLite database
    conn = sqlite3.connect('db1.db')

    # Execute the query to join videos and channels tables
    query = """
        SELECT videos.Video_title, channels.channel_name
        FROM videos
        JOIN channels ON channels.channel_id = videos.channel_id
    """
    df = pd.read_sql_query(query, conn)

    # Print the DataFrame in a tabular format
    print(df.to_string(index=False))

    # Close the connection
    conn.close()

# Call the function to fetch and print the data
fetch_video_and_channel_names()

import sqlite3
import pandas as pd

def fetch_channels_with_most_videos():
    # Connect to the SQLite database
    conn = sqlite3.connect('db1.db')

    # Execute the query to count videos per channel
    query = """
        SELECT channels.channel_name, COUNT(videos.video_id) AS video_count
        FROM channels
        JOIN videos ON channels.channel_id = videos.channel_id
        GROUP BY channels.channel_name
        ORDER BY video_count DESC
    """
    df = pd.read_sql_query(query, conn)

    # Print the DataFrame in a tabular format
    print(df.to_string(index=False))

    # Close the connection
    conn.close()

# Call the function to fetch and print the data
fetch_channels_with_most_videos()

import sqlite3
import pandas as pd

def fetch_top_10_viewed_videos():
    # Connect to the SQLite database
    conn = sqlite3.connect('db1.db')

    # Execute the query to get the top 10 most viewed videos
    query = """
        SELECT videos.Video_title, channels.channel_name
        FROM videos
        JOIN channels ON channels.channel_id = videos.channel_id
        ORDER BY videos.Video_viewcount DESC
        LIMIT 10
    """
    df = pd.read_sql_query(query, conn)

    # Print the DataFrame in a tabular format
    print(df.to_string(index=False))

    # Close the connection
    conn.close()

# Call the function to fetch and print the data
fetch_top_10_viewed_videos()

import sqlite3
import pandas as pd

def fetch_comments_per_video():
    # Connect to the SQLite database
    conn = sqlite3.connect('db1.db')

    # Execute the query to count comments per video
    query = """
        SELECT videos.Video_title, COUNT(comments.comment_id) AS comment_count
        FROM videos
        JOIN comments ON videos.video_id = comments.video_id
        GROUP BY videos.Video_title
    """
    df = pd.read_sql_query(query, conn)

    # Print the DataFrame in a tabular format
    print(df.to_string(index=False))

    # Close the connection
    conn.close()

# Call the function to fetch and print the data
fetch_comments_per_video()

import sqlite3
import pandas as pd

def fetch_most_liked_video():
    # Connect to the SQLite database
    conn = sqlite3.connect('db1.db')

    # Execute the query to find the most liked video
    query = """
        SELECT videos.Video_title, channels.channel_name
        FROM videos
        JOIN channels ON channels.channel_id = videos.channel_id
        ORDER BY videos.Video_likecount DESC
        LIMIT 1
    """
    df = pd.read_sql_query(query, conn)

    # Print the DataFrame in a tabular format
    print(df.to_string(index=False))

    # Close the connection
    conn.close()

# Call the function to fetch and print the data
fetch_most_liked_video()

import sqlite3
import pandas as pd

def fetch_total_likes_per_video():
    # Connect to the SQLite database
    conn = sqlite3.connect('db1.db')

    # Execute the query to calculate total likes per video
    query = """
        SELECT videos.Video_title, SUM(videos.Video_likecount) AS Total_likes
        FROM videos
        GROUP BY videos.Video_title
    """
    df = pd.read_sql_query(query, conn)

    # Print the DataFrame in a tabular format
    print(df.to_string(index=False))

    # Close the connection
    conn.close()

# Call the function to fetch and print the data
fetch_total_likes_per_video()

import sqlite3
import pandas as pd

def fetch_total_views_per_channel():
    # Connect to the SQLite database
    conn = sqlite3.connect('db1.db')

    # Execute the query to calculate total views per channel
    query = """
        SELECT channels.channel_name, SUM(videos.Video_viewcount) AS Total_views
        FROM channels
        JOIN videos ON channels.channel_id = videos.channel_id
        GROUP BY channels.channel_name
    """
    df = pd.read_sql_query(query, conn)

    # Print the DataFrame in a tabular format
    print(df.to_string(index=False))

    # Close the connection
    conn.close()

# Call the function to fetch and print the data
fetch_total_views_per_channel()

import sqlite3
import pandas as pd

def fetch_channels_published_in_2022():
    # Connect to the SQLite database
    conn = sqlite3.connect('db1.db')

    # Execute the query to find channels that published videos in 2022
    query = """
        SELECT DISTINCT channels.channel_name
        FROM channels
        JOIN videos ON channels.channel_id = videos.channel_id
        WHERE strftime('%Y', videos.Video_pubdate) = '2022'
    """
    df = pd.read_sql_query(query, conn)

    # Print the DataFrame in a tabular format
    print(df.to_string(index=False))

    # Close the connection
    conn.close()

# Call the function to fetch and print the data
fetch_channels_published_in_2022()

import sqlite3
import pandas as pd

def fetch_average_video_duration_per_channel():
    # Connect to the SQLite database
    conn = sqlite3.connect('db1.db')

    # Execute the query to calculate average video duration per channel
    query = """
        SELECT channels.channel_name, AVG(videos.Video_duration) AS Average_duration
        FROM channels
        JOIN videos ON channels.channel_id = videos.channel_id
        GROUP BY channels.channel_name
    """
    df = pd.read_sql_query(query, conn)

    # Print the DataFrame in a tabular format
    print(df.to_string(index=False))

    # Close the connection
    conn.close()

# Call the function to fetch and print the data
fetch_average_video_duration_per_channel()

import sqlite3
import pandas as pd

def fetch_most_commented_videos():
    # Connect to the SQLite database
    conn = sqlite3.connect('db1.db')

    # Execute the query to find the most commented video
    query = """
        SELECT videos.Video_title, channels.channel_name
        FROM videos
        JOIN channels ON channels.channel_id = videos.channel_id
        ORDER BY videos.Video_commentcount DESC
        LIMIT 1
    """
    df = pd.read_sql_query(query, conn)

    # Print the DataFrame in a tabular format
    print(df.to_string(index=False))

    # Close the connection
    conn.close()

# Call the function to fetch and print the data
fetch_most_commented_videos()


#Import necessary libraries
import streamlit as st
import pandas as pd
import googleapiclient.discovery
from googleapiclient.errors import HttpError
import re


# Define API version and service name
#Import necessary libraries
import streamlit as st
import pandas as pd
import googleapiclient.discovery
from googleapiclient.errors import HttpError
import re

api_service_name = "youtube"
api_version = "v3"
api_key="AIzaSyC50U1dJr0jYojCHsLCRXUIeDeceWSDsxw"

youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

import sqlite3
import pandas as pd

def fetch_data(query):
    # Connect to the SQLite database
    conn = sqlite3.connect('db1.db')

    # Read the query result into a pandas DataFrame
    df = pd.read_sql_query(query, conn)

    # Close the connection
    conn.close()

    return df

#Function to execute the predefined queries
import sqlite3
import pandas as pd

def execute_query(question):
    conn = sqlite3.connect('db1.db')  # Ensure this matches the DB used for table creation

    query_mapping = {
        "What are the names of all the videos and their corresponding channels?":
            """SELECT videos.Video_title, channels.channel_name
               FROM videos
               JOIN channels ON channels.channel_id = videos.channel_id;""",

        "Which channels have the most number of videos, and how many videos do they have?":
            """SELECT channels.channel_name, COUNT(videos.video_id) AS video_count
               FROM videos
               JOIN channels ON channels.channel_id = videos.channel_id
               GROUP BY channels.channel_name
               ORDER BY video_count DESC;""",

        "What are the top 10 most viewed videos and their respective channels?":
            """SELECT videos.video_title, channels.channel_name
               FROM videos
               JOIN channels ON channels.channel_id = videos.channel_id
               ORDER BY videos.video_viewcount DESC
               LIMIT 10;""",

        "How many comments were made on each video, and what are their corresponding video names?":
            """SELECT videos.video_title, COUNT(comments.comment_id) AS comment_count
               FROM videos
               JOIN comments ON videos.video_id = comments.video_id
               GROUP BY videos.video_title;""",

        "Which videos have the highest number of likes, and what are their corresponding channel names?":
            """SELECT videos.video_title, channels.channel_name
               FROM videos
               JOIN channels ON channels.channel_id = videos.channel_id
               ORDER BY videos.video_likecount DESC
               LIMIT 1;""",

        "What is the total number of likes for each video, and what are their corresponding video names?":
            """SELECT videos.video_title, SUM(videos.video_likecount) AS total_likes
               FROM videos
               GROUP BY videos.video_title;""",

        "What is the total number of views for each channel, and what are their corresponding channel names?":
            """SELECT channels.channel_name, SUM(videos.video_viewcount) AS total_views
               FROM videos
               JOIN channels ON channels.channel_id = videos.channel_id
               GROUP BY channels.channel_name;""",

        "What are the names of all the channels that have published videos in the year 2022?":
            """SELECT DISTINCT channels.channel_name
               FROM channels
               JOIN videos ON channels.channel_id = videos.channel_id
               WHERE strftime('%Y', videos.video_pubdate) = '2022';""",

        "What is the average duration of all videos in each channel, and what are their corresponding channel names?":
            """SELECT channels.channel_name, AVG(videos.video_duration) AS average_duration
               FROM videos
               JOIN channels ON videos.channel_id = channels.channel_id
               GROUP BY channels.channel_name;""",

        "Which videos have the highest number of comments, and what are their corresponding channel names?":
            """SELECT videos.video_title, channels.channel_name
               FROM videos
               JOIN channels ON videos.channel_id = channels.channel_id
               ORDER BY videos.video_commentcount DESC
               LIMIT 1;"""
    }

    query = query_mapping.get(question)
    if query:
        df = pd.read_sql_query(query, conn)
    else:
        df = pd.DataFrame()

    conn.close()
    return df


import sqlite3
import pandas as pd

def fetch_channel_data(newchannel_id):
    # Connect to the SQLite database
    conn = sqlite3.connect('db1.db')

    # Execute the query to check if the channel exists
    query = "SELECT * FROM channels WHERE channel_id = ?"
    df = pd.read_sql_query(query, conn, params=(newchannel_id,))

    # Check if the channel exists
    if not df.empty:
        existing_channel = df.iloc[0]
        print("Channel already exists in the database.")
        return existing_channel

    # Close the connection
    conn.close()

    # If the channel doesn't exist, fetch data using the API key (implement your API logic here)
    # ... (Your API fetching logic)
    # ...
    # Assuming you've fetched the channel data into a DataFrame 'new_channel_data'
    new_channel_data.to_sql('channels', conn, if_exists='append', index=False)

    return new_channel_data


import sqlite3
import pandas as pd

def fetch_channel_data(newchannel_id):
    # Connect to the SQLite database
    conn = sqlite3.connect('db1.db')

    # Execute the query to check if the channel exists
    query = "SELECT * FROM channels WHERE channel_id = ?"
    df = pd.read_sql_query(query, conn, params=(newchannel_id,))

    # Check if the channel exists
    if not df.empty:
        existing_channel = df.iloc[0]
        print("Channel already exists in the database.")
        return existing_channel

    # Close the connection
    conn.close()

    # If the channel doesn't exist, fetch data using the API key (implement your API logic here)
    try:
        # Your API fetching logic here
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=newchannel_id
        )
        response = request.execute()

        if 'items' in response and len(response["items"]) > 0:
            data = {
                "channel_name": response["items"][0]["snippet"]["title"],
                "channel_id": newchannel_id,
                "channel_des": response["items"][0]["snippet"]["description"],
                "channel_playid": response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"],
                "channel_viewcount": response["items"][0]["statistics"]["viewCount"],
                "channel_subcount": response["items"][0]["statistics"]["subscriberCount"]
            }

            # Insert the fetched data into the SQLite database
            new_channel_data = pd.DataFrame(data, index=[0])
            new_channel_data.to_sql('channels', conn, if_exists='append', index=False)

            return new_channel_data

        else:
            print("No items found in the response.")
            return pd.DataFrame()

    except HttpError as e:
        print(f"HTTP Error: {e}")
        return pd.DataFrame()
    except KeyError as e:
        print(f"KeyError: {e}. Please make sure the channel ID is correct.")
        return pd.DataFrame()

#Function to fetch the video ID using channel ID
def playlist_videos_id(channel_ids):
    all_video_ids=[]
    for newchannel_id in channel_ids:
        videos_ids=[]
        try:
            response = youtube.channels().list(part="contentDetails",id=newchannel_id).execute()
            if 'items' in response and len(response["items"]) > 0:
                playlist_Id=response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
                nextPageToken=None

                while True:
                    response2 = youtube.playlistItems().list(
                       part="snippet",
                       playlistId=playlist_Id,maxResults=50,
                       pageToken=nextPageToken).execute()
                    for i in range(len(response2.get("items",[]))):
                       videos_ids.append(response2["items"][i]["snippet"]["resourceId"]["videoId"])
                    nextPageToken=response2.get("nextPageToken")
                    if nextPageToken is None:
                        break
                else:
                    st.error(f"No channels found for ID: {newchannel_id}")
        except HttpError as e:
            st.error(f"HTTP Error: {e}")
        except KeyError as e:
            st.error(f"KeyError: {e}")

        all_video_ids.extend(videos_ids)
    return all_video_ids


#Function to fetch the video datas from the video_IDs
def fetch_video_data(all_video_ids):
    video_info=[]
    for each in all_video_ids:
        request = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=each

        )
        response = request.execute()
        for i in response["items"]:
            given= {
                       "Video_Id":i["id"] ,
                       "Video_title":i["snippet"]["title"],
                       "Video_Description":i["snippet"]["description"],
                       "channel_id":i['snippet']['channelId'],
                       "video_Tags": i['snippet'].get("Tags",0),
                       "Video_pubdate":i["snippet"]["publishedAt"],
                       "Video_viewcount":i["statistics"]["viewCount"],
                       "Video_likecount":i["statistics"].get('likeCount',0) ,
                       "Video_favoritecount":i["statistics"]["favoriteCount"],
                       "Video_commentcount":i["statistics"].get("Comment_Count",0),
                       "Video_duration":iso8601_duration_to_seconds(i["contentDetails"]["duration"]),
                       "Video_thumbnails":i["snippet"]["thumbnails"]['default']['url'],
                       "Video_caption":i["contentDetails"]["caption"]
            }

            video_info.append(given)


import sqlite3
import pandas as pd

def insert_video_info(video_info):
    # Connect to the SQLite database
    conn = sqlite3.connect('db1.db')

    # Create a cursor object to execute SQL commands
    cursor = conn.cursor()

    # Create the 'videos' table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS videos (
            Video_Id TEXT,
            Video_title TEXT,
            Video_Description TEXT,
            channel_id TEXT,
            video_Tags TEXT,
            Video_pubdate TEXT,
            Video_viewcount INTEGER,
            Video_likecount INTEGER,
            Video_favoritecount INTEGER,
            Video_commentcount INTEGER,
            Video_duration INTEGER,
            Video_thumbnails TEXT,
            Video_caption TEXT
        )
    ''')

    # Insert the video data into the table
    for video in video_info:
        cursor.execute("""
            INSERT INTO videos (Video_Id, Video_title, Video_Description, channel_id, video_Tags, Video_pubdate, Video_viewcount,
            Video_likecount, Video_favoritecount, Video_commentcount, Video_duration, Video_thumbnails, Video_caption)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (video['Video_Id'], video['Video_title'], video['Video_Description'], video['channel_id'], video['video_Tags'],
               video['Video_pubdate'], video['Video_viewcount'], video['Video_likecount'], video['Video_favoritecount'],
               video['Video_commentcount'], video['Video_duration'], video['Video_thumbnails'], video['Video_caption']))

    # Commit the changes to the database
    conn.commit()

    # Close the connection
    conn.close()

    return pd.DataFrame(video_info)

#Function to fetch the comments from video IDs
def Fetch_comment_data(newchannel_id):
    commentdata=[]
    allvideo_ids=playlist_videos_id([newchannel_id])
    for video in allvideo_ids:

            try:
                request=youtube.commentThreads().list(
                        part="snippet",
                        videoId=video,
                        maxResults=50)
                response=request.execute()
                for all in response["items"]:
                    given={
                                        "comment_id":all["snippet"]["topLevelComment"]["id"],
                                        "Comment_Text":all["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                                        "Comment_Authorname":all["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                        "published_date":all["snippet"]["topLevelComment"]["snippet"]["publishedAt"],
                                        "video_id":all["snippet"]["topLevelComment"]["snippet"]["videoId"],
                                        'channel_id': all['snippet']['channelId']}

                    commentdata.append(given)
                nextpagetoken= response.get('nextPageToken')
            except HttpError as e:
                pass
		

import sqlite3
import pandas as pd

def insert_comment_data(commentdata):
    # Connect to the SQLite database
    conn = sqlite3.connect('db1.db')

    # Create a cursor object to execute SQL commands
    cursor = conn.cursor()

    # Create the 'comments' table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS comments (
            comment_id TEXT,
            Comment_Text TEXT,
            Comment_Authorname TEXT,
            published_date TEXT,
            video_id TEXT,
            channel_id TEXT
        )
    ''')

    # Insert the comment data into the table
    for comment in commentdata:
        cursor.execute("""
            INSERT INTO comments (comment_id, Comment_Text, Comment_Authorname, published_date, video_id, channel_id)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (comment['comment_id'], comment['Comment_Text'], comment['Comment_Authorname'], comment['published_date'],
               comment['video_id'], comment['channel_id']))

    # Commit the changes to the database
    conn.commit()

    # Close the connection
    conn.close()

    return pd.DataFrame(commentdata)

#function to convert the duration from hours to seconds
def iso8601_duration_to_seconds(duration):
    match = re.match(r'^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$', duration)
    if not match:
        return None

    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0

    total_seconds = (hours * 3600) + (minutes * 60) + seconds
    return total_seconds





#Streamlit coding part to showcase the output in streamlit
import streamlit as st
def main():
    st.title("YouTube Data Harvesting and Warehousing using SQL and Streamlit")
    st.sidebar.header("Tables")

    Options = st.sidebar.radio("Options", ("Channels", "Videos", "Comments", "Queries", "Enter YouTube Channel ID"))

    if  Options == "Channels":
        st.header("Channels")
        channels_df = fetch_data("SELECT * FROM Channels;")
        channels_df.index += 1
        st.dataframe(channels_df)

    elif Options == "Videos":
        st.header("Videos")
        videos_df = fetch_data("SELECT * FROM Videos;")
        videos_df.index += 1
        st.dataframe(videos_df)

    elif Options == "Comments":
        st.header("Comments")
        comments_df = fetch_data("SELECT * FROM Comments;")
        comments_df.index += 1
        st.dataframe(comments_df)

    elif Options == "Queries":
        st.header("Queries")
        query_question = st.selectbox("Select Query", [
            "What are the names of all the videos and their corresponding channels?",
            "Which channels have the most number of videos, and how many videos do they have?",
            "What are the top 10 most viewed videos and their respective channels?",
            "How many comments were made on each video, and what are their corresponding video names?",
            "Which videos have the highest number of likes, and what are their corresponding channel names?",
            "What is the total number of likes for each video, and what are their corresponding video names?",
            "What is the total number of views for each channel, and what are their corresponding channel names?",
            "What are the names of all the channels that have published videos in the year 2022?",
            "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
            "Which videos have the highest number of comments, and what are their corresponding channel names?"])

        if query_question:
            query_result_df = execute_query(query_question)
            query_result_df.index += 1
            st.dataframe(query_result_df)
    elif Options == "Enter YouTube Channel ID":
        st.header("Enter YouTube Channel ID")
        channel_id = st.text_input("Channel ID")
        if st.button("Fetch Channel Data"):
            channel_df = fetch_channel_data(channel_id)
            channel_df.index +=1
            st.subheader("Channel Data")
            st.write(channel_df)

        if st.button("Fetch Video Data"):
            all_video_ids = playlist_videos_id([channel_id])
            video_df = fetch_video_data(all_video_ids)
            video_df.index +=1
            st.subheader("Video Data")
            st.write(video_df)

        if st.button("Fetch Comment Data"):
            comment_df = Fetch_comment_data(channel_id)
            comment_df.index +=1
            st.subheader("Comment Data")
            st.write(comment_df)


if __name__ == "__main__":
    main()

