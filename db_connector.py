import psycopg2
from urllib.parse import urlparse
from datetime import datetime
import os
import logging

# تهيئة التسجيل
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_db():
    try:
        url = urlparse(os.getenv("DATABASE_URL"))
        conn = psycopg2.connect(
            database=url.path[1:],
            user=url.username,
            password=url.password,
            host=url.hostname,
            port=url.port
        )
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS updates (
                creative_id TEXT,
                new_headline TEXT,
                status TEXT,
                error_message TEXT,
                timestamp TEXT
            )
        ''')
        conn.commit()
        logger.info("تم تهيئة قاعدة البيانات بنجاح")
        return conn
    except Exception as e:
        logger.error(f"فشل تهيئة قاعدة البيانات: {str(e)}")
        return None

def log_update(conn, creative_id, new_headline, status, error_message):
    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO updates (creative_id, new_headline, status, error_message, timestamp) VALUES (%s, %s, %s, %s, %s)",
            (creative_id, new_headline, status, error_message, datetime.now().isoformat())
        )
        conn.commit()
        logger.info(f"تم تسجيل التحديث لـ {creative_id}")
    except Exception as e:
        logger.error(f"فشل تسجيل التحديث: {str(e)}")

def close_db(conn):
    if conn:
        conn.close()
        logger.info("تم إغلاق قاعدة البيانات")
