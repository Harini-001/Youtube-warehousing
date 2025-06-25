
import sqlite3

def ensure_tables():
    conn = sqlite3.connect("db1.db")
    cursor = conn.cursor()

    cursor.execute("""
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
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS channels (
            channel_name TEXT,
            channel_id TEXT,
            channel_des TEXT,
            channel_playid TEXT,
            channel_viewcount INTEGER,
            channel_subcount INTEGER
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS comments (
            comment_id TEXT,
            Comment_Text TEXT,
            comment_authorname TEXT,
            published_date TEXT,
            video_id TEXT,
            channel_id TEXT
        )
    """)

    conn.commit()
    conn.close()

if __name__ == "__main__":
    ensure_tables()
    print("✅ All required tables have been created in db1.db")
