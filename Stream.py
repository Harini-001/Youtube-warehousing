import streamlit as st
import pandas as pd
import numpy as np # Not used in final code, can remove
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import re
import json # Not used in final code, can remove
import os # For environment variables
import sqlite3
import time # For backoff

# --- Configuration & API Setup ---
st.title('📺 YouTube Data Harvesting and Warehousing')
st.markdown('### Built with Streamlit')

# Load API key from environment variable
API_KEY = os.getenv("YOUTUBE_API_KEY")
if not API_KEY:
    st.error("Error: YouTube API Key not found. Please set the YOUTUBE_API_KEY environment variable. "
             "Example: `export YOUTUBE_API_KEY=\"YOUR_API_KEY_HERE\"` (Linux/macOS) or "
             "`$env:YOUTUBE_API_KEY=\"YOUR_API_KEY_HERE\"` (Windows PowerShell)")
    st.stop() # Stop the Streamlit app if API key is missing

api_service_name = "youtube"
api_version = "v3"

def Api_connector():
    try:
        return build(api_service_name, api_version, developerKey=API_KEY)
    except Exception as e:
        st.error(f"Failed to connect to YouTube API: {e}")
        st.stop()

youtube = Api_connector()

# --- SQLite Database Setup (Initial run or on app start) ---
# Ensure this runs only once or when you need to create/recreate tables
conn = sqlite3.connect("db1.db")
cursor = conn.cursor()

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
        Video_duration INTEGER,
        Video_thumbnails TEXT,
        Video_caption TEXT
    )
''')
cursor.execute('''
    CREATE TABLE IF NOT EXISTS channels (
        channel_name TEXT,
        channel_id TEXT PRIMARY KEY,
        channel_des TEXT,
        channel_playid TEXT,
        channel_viewcount INTEGER,
        channel_subcount INTEGER
    )
''')
cursor.execute('''
    CREATE TABLE IF NOT EXISTS comments (
        comment_id TEXT PRIMARY KEY,
        Comment_Text TEXT,
        Comment_Authorname TEXT,
        published_date TEXT,
        video_id TEXT,
        channel_id TEXT
    )
''')
conn.commit()
conn.close()

# --- Helper for API Calls with Backoff ---
def safe_api_call(request_callable, *args, **kwargs):
    retries = 0
    max_retries = 5
    initial_delay = 1 # seconds

    while retries < max_retries:
        try:
            return request_callable(*args, **kwargs).execute()
        except HttpError as e:
            if e.resp.status == 403 and "quotaExceeded" in str(e):
                delay = initial_delay * (2 ** retries)
                st.warning(f"Quota exceeded. Retrying in {delay:.1f} seconds (Attempt {retries + 1}/{max_retries})...")
                time.sleep(delay)
                retries += 1
            elif e.resp.status == 404:
                st.warning(f"Resource not found (404) for request. Skipping. Error: {e}")
                return None
            else:
                st.error(f"An unexpected API error occurred: {e}")
                raise
        except Exception as e:
            st.error(f"An unexpected error occurred during API call: {e}")
            raise
    st.error(f"Failed after {max_retries} retries due to quota issues. Please check your Google Cloud Console for quota status.")
    return None

# --- Existing Functions (modified to use safe_api_call and error handling) ---

def channel_info(channel_id):
    request = youtube.channels().list(
                    part="snippet,contentDetails,statistics",
                    id=channel_id
    )
    response = safe_api_call(request.execute)
    if response and response.get("items"):
        data = {
                            "Channel_Name": response["items"][0]["snippet"]["title"],
                            "Channel_Id": response["items"][0]["id"],
                            "Channel_Des": response["items"][0]["snippet"]["description"],
                            "Channel_playid": response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"],
                            "channel_viewcount": response["items"][0]["statistics"]["viewCount"],
                            "channel_subcount": response["items"][0]["statistics"]["subscriberCount"]
                             }
        return pd.DataFrame(data,index=[0])
    st.warning(f"No channel data found for ID: {channel_id}")
    return pd.DataFrame()

def eachchanneldetails(channel_ids):
    conn = sqlite3.connect('db1.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS channels (
            channel_name TEXT,
            channel_id TEXT PRIMARY KEY,
            channel_des TEXT,
            channel_playid TEXT,
            channel_viewcount INTEGER,
            channel_subcount INTEGER
        )
    ''')
    conn.commit()

    for channel_id in channel_ids:
        df = channel_info(channel_id)
        if not df.empty:
            try:
                df.to_sql('channels', conn, if_exists='append', index=False)
                st.success(f"✅ Channel '{df['Channel_Name'].iloc[0]}' data inserted.")
            except sqlite3.IntegrityError:
                st.info(f"Channel '{df['Channel_Name'].iloc[0]}' (ID: {channel_id}) already exists. Skipping insertion.")
            except Exception as e:
                st.error(f"Error inserting channel {channel_id} data: {e}")
        else:
            st.warning(f"Could not fetch data for channel ID: {channel_id}")
    conn.close()


def playlist_videos_id(channel_ids):
    all_video_ids = []
    for current_channel_id in channel_ids: # Renamed from channels_id to avoid conflict
        videos_ids = []
        st.info(f"Fetching playlist for channel: {current_channel_id}")
        response = safe_api_call(youtube.channels().list(part="contentDetails",id=current_channel_id).execute)
        
        if response and 'items' in response and len(response["items"]) > 0:
            playlist_Id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
            nextPageToken = None

            while True:
                response2 = safe_api_call(youtube.playlistItems().list(
                   part="snippet",
                   playlistId=playlist_Id, maxResults=50,
                   pageToken=nextPageToken).execute)
                
                if response2 is None: # safe_api_call returned None
                    break

                for i in range(len(response2.get("items", []))):
                    videos_ids.append(response2["items"][i]["snippet"]["resourceId"]["videoId"])
                
                nextPageToken = response2.get("nextPageToken")
                if nextPageToken is None:
                    break
        else:
            st.error(f"No content details found for channel ID: {current_channel_id}. It might be invalid or restricted.")
        all_video_ids.extend(videos_ids)
    return all_video_ids

def iso8601_duration_to_seconds(duration):
    match = re.match(r'^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$', duration)
    if not match:
        return 0 # Default to 0 seconds if format is unexpected

    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0

    total_seconds = (hours * 3600) + (minutes * 60) + seconds
    return total_seconds

def videos_data(video_ids):
    st.subheader("Fetching Video Details")
    all_video_stats = []

    if not video_ids:
        st.warning("No video IDs provided to fetch data.")
        return pd.DataFrame()

    # Process video IDs in batches of 50 to optimize quota (1 unit per 50 videos)
    for i in range(0, len(video_ids), 50):
        batch_ids = video_ids[i:i+50]
        st.write(f"▶️ Fetching batch of video IDs: `{', '.join(batch_ids)}`")
        
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(batch_ids)
        )
        
        response = safe_api_call(request.execute)

        if response and response.get("items"):
            for item in response["items"]:
                snippet = item.get("snippet", {})
                statistics = item.get("statistics", {})
                contentDetails = item.get("contentDetails", {})

                video_info = {
                    "Video_Id": item.get("id"),
                    "Video_title": snippet.get("title"),
                    "Video_Description": snippet.get("description"),
                    "channel_id": snippet.get("channelId"),
                    "video_tags": ', '.join(snippet.get("tags", [])), # Store as comma-separated string
                    "Video_pubdate": snippet.get("publishedAt"),
                    "Video_viewcount": int(statistics.get("viewCount", 0)),
                    "Video_likecount": int(statistics.get("likeCount", 0)),
                    "Video_favoritecount": int(statistics.get("favoriteCount", 0)),
                    "Video_commentcount": int(statistics.get("commentCount", 0)), # Corrected key
                    "Video_duration": iso8601_duration_to_seconds(contentDetails.get("duration", "PT0S")),
                    "Video_thumbnails": snippet.get("thumbnails", {}).get("default", {}).get("url"),
                    "Video_caption": contentDetails.get("caption", "false") # 'false' if no caption
                }
                all_video_stats.append(video_info)
        else:
            st.warning(f"No data returned for video batch: {batch_ids}. Could be invalid IDs or quota issues.")

    return pd.DataFrame(all_video_stats)


def insert_videos_into_sqlite(df1):
    conn = sqlite3.connect('db1.db')
    cursor = conn.cursor()
    # Ensure table creation matches schema and primary key
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
            Video_duration INTEGER,
            Video_thumbnails TEXT,
            Video_caption TEXT
        )
    ''')
    conn.commit()

    if not df1.empty:
        try:
            # Use to_sql with if_exists='append' and handle duplicates via primary key
            df1.to_sql('videos', conn, if_exists='append', index=False)
            st.success(f"✅ Successfully inserted {len(df1)} video records into 'videos' table!")
        except Exception as e:
            st.error(f"Error inserting video data: {e}")
            # You might want more granular error handling here for specific SQLite errors
    else:
        st.warning("No video data to insert.")
    conn.close()

def comments_inf(video_ids, current_channel_id_for_comments): # Pass channel_id here
    commentdata = []
    
    if not video_ids:
        st.warning("No video IDs provided to fetch comments.")
        return pd.DataFrame()

    st.subheader(f"Fetching comments for {len(video_ids)} videos...")

    for video_id in video_ids:
        st.write(f"💬 Fetching comments for video ID: `{video_id}`")
        nextpagetoken = None
        
        while True:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50,
                pageToken=nextpagetoken
            )
            
            response = safe_api_call(request.execute)

            if response is None:
                break 

            # Check for comments disabled error (specific 403 reason)
            if "error" in response and response["error"]["code"] == 403:
                # Specific check for comments disabled error
                if any("commentsDisabled" in detail.get("reason", "") for detail in response["error"].get("errors", [])):
                    st.warning(f"⚠️ Comments are disabled for video ID: {video_id}. Skipping.")
                    break
                else:
                    st.error(f"Unhandled 403 API error for video {video_id}: {response['error']}")
                    break # Break for other 403 errors too

            if response.get("items"):
                for item in response["items"]:
                    top_level_comment_snippet = item.get("snippet", {}).get("topLevelComment", {}).get("snippet", {})
                    # The channelId in commentThreads().list response snippet refers to the video's channel
                    # So it's fine to pass current_channel_id_for_comments which should be the video's channel
                    
                    comment = {
                        "comment_id": item.get("id"),
                        "Comment_Text": top_level_comment_snippet.get("textDisplay"),
                        "Comment_Authorname": top_level_comment_snippet.get("authorDisplayName"),
                        "published_date": top_level_comment_snippet.get("publishedAt"),
                        "video_id": top_level_comment_snippet.get("videoId"),
                        "channel_id": current_channel_id_for_comments
                    }
                    commentdata.append(comment)
                
                nextpagetoken = response.get('nextPageToken')
                if not nextpagetoken:
                    break
            else:
                st.info(f"No more comments found for video ID: {video_id} or end of pages.")
                break
                
    return pd.DataFrame(commentdata)

def insert_comments_into_sqlite(df2):
    conn = sqlite3.connect('db1.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS comments (
            comment_id TEXT PRIMARY KEY,
            Comment_Text TEXT,
            Comment_Authorname TEXT,
            published_date TEXT,
            video_id TEXT,
            channel_id TEXT
        )
    ''')
    conn.commit()

    if not df2.empty:
        try:
            df2.to_sql('comments', conn, if_exists='append', index=False)
            st.success(f"✅ Successfully inserted {len(df2)} comment records into 'comments' table!")
        except Exception as e:
            st.error(f"Error inserting comment data: {e}")
    else:
        st.warning("No comment data to insert.")
    conn.close()

# --- Streamlit Main App Logic ---
def main():
    st.sidebar.header("Data Operations")

    Options = st.sidebar.radio("Go to:", ("View Tables", "Perform Queries", "Enter YouTube Channel ID"))

    if Options == "View Tables":
        st.header("View Existing Tables")
        table_choice = st.selectbox("Select Table to View", ["channels", "videos", "comments"])
        conn = sqlite3.connect("db1.db")
        try:
            df = pd.read_sql(f"SELECT * FROM {table_choice}", conn)
            if df.empty:
                st.warning(f"⚠️ {table_choice.capitalize()} table exists, but it's empty.")
            else:
                df.index += 1
                st.dataframe(df)
        except pd.io.sql.DatabaseError as e:
            st.error(f"❌ Error accessing {table_choice} table: {e}. It might not exist yet.")
        finally:
            conn.close()

    elif Options == "Perform Queries":
        st.header("Run Predefined Queries")
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
            with st.spinner("Executing query..."):
                query_result_df = execute_query(query_question)
            if not query_result_df.empty:
                query_result_df.index += 1
                st.dataframe(query_result_df)
            else:
                st.warning("No results found for this query, or data tables are empty.")

    elif Options == "Enter YouTube Channel ID":
        st.header("Harvest Data from YouTube Channel")
        current_channel_id = st.text_input("Enter YouTube Channel ID:")

        if st.button("Fetch & Store Channel Data"):
            if current_channel_id:
                with st.spinner(f"Fetching data for channel ID: {current_channel_id}..."):
                    channel_df = channel_info(current_channel_id)
                    if not channel_df.empty:
                        conn = sqlite3.connect('db1.db')
                        try:
                            channel_df.to_sql('channels', conn, if_exists='append', index=False)
                            st.success(f"✅ Channel '{channel_df['Channel_Name'].iloc[0]}' data inserted/updated!")
                            st.dataframe(channel_df.iloc[:,[0,1,4,5]].style.format({'channel_viewcount': "{:,}", 'channel_subcount': "{:,}"}))
                        except sqlite3.IntegrityError:
                            st.info(f"Channel '{channel_df['Channel_Name'].iloc[0]}' (ID: {current_channel_id}) already exists. No new insertion.")
                        except Exception as e:
                            st.error(f"Error saving channel data: {e}")
                        finally:
                            conn.close()
                    else:
                        st.warning("⚠️ No channel data fetched. Check ID or API quota.")
            else:
                st.error("Please enter a Channel ID first.")

        # --- Fetch Video Data Button ---
        if st.button("Fetch & Store Video Data"):
            if current_channel_id:
                with st.spinner(f"Fetching video IDs for channel: {current_channel_id} (This might take a while for large channels)..."):
                    video_ids = playlist_videos_id([current_channel_id])
                
                if video_ids:
                    st.info(f"Found {len(video_ids)} video IDs. Now fetching detailed video data...")
                    with st.spinner("Fetching detailed video data and inserting into DB..."):
                        videos_df_to_insert = videos_data(video_ids) # This now returns the list of dicts
                        if not videos_df_to_insert.empty:
                            insert_videos_into_sqlite(videos_df_to_insert) # Pass the DataFrame directly
                        else:
                            st.warning("No video data could be fetched for insertion.")
                else:
                    st.warning("No video IDs found for this channel or API error occurred.")
            else:
                st.error("Please enter a Channel ID first.")

        # --- Fetch Comment Data Button ---
        if st.button("Fetch & Store Comment Data"):
            if current_channel_id:
                with st.spinner(f"Fetching video IDs for comments from channel: {current_channel_id}..."):
                    video_ids_for_comments = playlist_videos_id([current_channel_id])
                
                if video_ids_for_comments:
                    st.info(f"Found {len(video_ids_for_comments)} video IDs. Now fetching comments...")
                    with st.spinner("Fetching comment data and inserting into DB (This can be very slow and quota-heavy!)..."):
                        comments_df_to_insert = comments_inf(video_ids_for_comments, current_channel_id) # Pass channel_id
                        if not comments_df_to_insert.empty:
                            insert_comments_into_sqlite(comments_df_to_insert)
                        else:
                            st.warning("No comment data could be fetched for insertion.")
                else:
                    st.warning("No video IDs found for comment fetching or API error occurred.")
            else:
                st.error("Please enter a Channel ID first.")

# Function to execute predefined queries (kept for consistency with your code)
def execute_query(question):
    conn = sqlite3.connect('db1.db')
    query_mapping = {
        "What are the names of all the videos and their corresponding channels?":
            """SELECT videos.Video_title, channels.channel_name
               FROM videos
               JOIN channels ON channels.channel_id = videos.channel_id;""",

        "Which channels have the most number of videos, and how many videos do they have?":
            """SELECT channels.channel_name, COUNT(videos.Video_Id) AS video_count
               FROM videos
               JOIN channels ON channels.channel_id = videos.channel_id
               GROUP BY channels.channel_name
               ORDER BY video_count DESC;""",

        "What are the top 10 most viewed videos and their respective channels?":
            """SELECT videos.Video_title, channels.channel_name
               FROM videos
               JOIN channels ON channels.channel_id = videos.channel_id
               ORDER BY videos.Video_viewcount DESC
               LIMIT 10;""",

        "How many comments were made on each video, and what are their corresponding video names?":
            """SELECT videos.Video_title, COUNT(comments.comment_id) AS comment_count
               FROM videos
               JOIN comments ON videos.Video_Id = comments.video_id
               GROUP BY videos.Video_title;""",

        "Which videos have the highest number of likes, and what are their corresponding channel names?":
            """SELECT videos.Video_title, channels.channel_name
               FROM videos
               JOIN channels ON channels.channel_id = videos.channel_id
               ORDER BY videos.Video_likecount DESC
               LIMIT 1;""",

        "What is the total number of likes for each video, and what are their corresponding video names?":
            """SELECT videos.Video_title, SUM(videos.Video_likecount) AS total_likes
               FROM videos
               GROUP BY videos.Video_title;""",

        "What is the total number of views for each channel, and what are their corresponding channel names?":
            """SELECT channels.channel_name, SUM(videos.Video_viewcount) AS total_views
               FROM videos
               JOIN channels ON channels.channel_id = videos.channel_id
               GROUP BY channels.channel_name;""",

        "What are the names of all the channels that have published videos in the year 2022?":
            """SELECT DISTINCT channels.channel_name
               FROM channels
               JOIN videos ON channels.channel_id = videos.channel_id
               WHERE strftime('%Y', videos.Video_pubdate) = '2022';""",

        "What is the average duration of all videos in each channel, and what are their corresponding channel names?":
            """SELECT channels.channel_name, AVG(videos.Video_duration) AS average_duration
               FROM videos
               JOIN channels ON videos.channel_id = channels.channel_id
               GROUP BY channels.channel_name;""",

        "Which videos have the highest number of comments, and what are their corresponding channel names?":
            """SELECT videos.Video_title, channels.channel_name
               FROM videos
               JOIN channels ON videos.channel_id = channels.channel_id
               ORDER BY videos.Video_commentcount DESC
               LIMIT 1;"""
    }

    query = query_mapping.get(question)
    if query:
        df = pd.read_sql_query(query, conn)
    else:
        df = pd.DataFrame()
        st.error("Invalid query selected.")

    conn.close()
    return df

def fetch_channel_data(newchannel_id):
    conn = sqlite3.connect('db1.db')
    query = "SELECT * FROM channels WHERE channel_id = ?"
    df = pd.read_sql_query(query, conn, params=(newchannel_id,))

    if not df.empty:
        st.info("Channel already exists in the database. Returning existing data.")
        conn.close()
        return df # Return the DataFrame directly

    st.info(f"Channel {newchannel_id} not found in DB. Attempting to fetch from YouTube API...")
    try:
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=newchannel_id
        )
        response = safe_api_call(request.execute) # Use safe_api_call

        if response and 'items' in response and len(response["items"]) > 0:
            data = {
                "channel_name": response["items"][0]["snippet"]["title"],
                "channel_id": newchannel_id,
                "channel_des": response["items"][0]["snippet"]["description"],
                "channel_playid": response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"],
                "channel_viewcount": int(response["items"][0]["statistics"]["viewCount"]),
                "channel_subcount": int(response["items"][0]["statistics"]["subscriberCount"])
            }
            new_channel_data = pd.DataFrame(data, index=[0])
            
            # Insert the fetched data into the SQLite database
            try:
                new_channel_data.to_sql('channels', conn, if_exists='append', index=False)
                st.success(f"✅ Channel '{new_channel_data['channel_name'].iloc[0]}' inserted into database.")
            except sqlite3.IntegrityError:
                 st.info(f"Channel '{new_channel_data['channel_name'].iloc[0]}' (ID: {newchannel_id}) already exists. No new insertion.")
            except Exception as e:
                st.error(f"Error inserting new channel data into DB: {e}")

            conn.close()
            return new_channel_data
        else:
            st.warning(f"No items found in the API response for channel ID: {newchannel_id}. It might be invalid or not public.")
            conn.close()
            return pd.DataFrame()

    except Exception as e:
        st.error(f"Error fetching channel data from API: {e}")
        conn.close()
        return pd.DataFrame()

if __name__ == "__main__":
    main()
