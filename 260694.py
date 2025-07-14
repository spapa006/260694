import requests
import json
import random
import logging
import sqlite3
import time
from datetime import datetime
from dotenv import load_dotenv
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# إعداد التسجيل
logging.basicConfig(
    filename='snapchat_ads_bot.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# إعداد جلسة HTTP مع إعادة المحاولة
session = requests.Session()
retries = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
session.mount('https://', HTTPAdapter(max_retries=retries))

# تحميل متغيرات البيئة
load_dotenv()

# متغير عالمي للعناوين
headlines = []

# تهيئة قاعدة البيانات
def init_db():
    """Initialize SQLite database for logging updates."""
    try:
        conn = sqlite3.connect('snapchat_ads.db')
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS updates (
            creative_id TEXT,
            old_headline TEXT,
            new_headline TEXT,
            status TEXT,
            error_message TEXT,
            timestamp TEXT
        )''')
        conn.commit()
        conn.close()
        logging.info("Database initialized successfully")
    except sqlite3.Error as e:
        logging.error(f"Failed to initialize database: {e}")
        if str(e) == "file is not a database":
            logging.info("Attempting to recreate database file")
            try:
                os.remove('snapchat_ads.db')
                conn = sqlite3.connect('snapchat_ads.db')
                c = conn.cursor()
                c.execute('''CREATE TABLE updates (
                    creative_id TEXT,
                    old_headline TEXT,
                    new_headline TEXT,
                    status TEXT,
                    error_message TEXT,
                    timestamp TEXT
                )''')
                conn.commit()
                conn.close()
                logging.info("Database recreated successfully")
            except Exception as recreate_e:
                logging.error(f"Failed to recreate database: {recreate_e}")
                raise
        else:
            raise

# تسجيل التحديثات
def log_update(creative_id, old_headline, new_headline, status, error_message):
    """Log creative update details to the database."""
    try:
        conn = sqlite3.connect('snapchat_ads.db')
        c = conn.cursor()
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        c.execute('''INSERT INTO updates (creative_id, old_headline, new_headline, status, error_message, timestamp)
                     VALUES (?, ?, ?, ?, ?, ?)''',
                  (creative_id, old_headline, new_headline, status, error_message, timestamp))
        conn.commit()
        conn.close()
        logging.info(f"Logged update for creative {creative_id}: {status}")
    except sqlite3.Error as e:
        logging.error(f"Failed to log update for creative {creative_id}: {e}")

# توليد عنوان جديد
def generate_new_headline(creative_id, old_headline):
    """Generate a new headline, avoiding duplicates."""
    global headlines
    try:
        if not headlines:
            with open('headline.txt', 'r', encoding='utf-8') as file:
                headlines = [line.strip() for line in file if line.strip()]
            logging.info("Headlines reloaded from file")

        # استبعاد العناوين المستخدمة مسبقًا
        conn = sqlite3.connect('snapchat_ads.db')
        c = conn.cursor()
        c.execute("SELECT new_headline FROM updates WHERE creative_id=?", (creative_id,))
        used_headlines = [row[0] for row in c.fetchall()]
        conn.close()

        available_headlines = [h for h in headlines if h not in used_headlines and h != old_headline]
        if not available_headlines:
            logging.warning(f"No unique headlines available for {creative_id}, reloading all headlines")
            with open('headline.txt', 'r', encoding='utf-8') as file:
                headlines = [line.strip() for line in file if line.strip()]
            available_headlines = [h for h in headlines if h != old_headline]
            if not available_headlines:
                logging.warning(f"No unique headlines available for {creative_id}, reusing all headlines")
                available_headlines = headlines

        selected_headline = random.choice(available_headlines)
        headlines.remove(selected_headline)
        if len(selected_headline) > 40:
            selected_headline = selected_headline[:37] + "..."
        logging.info(f"Selected headline for {creative_id}: {selected_headline}")
        return selected_headline
    except Exception as e:
        logging.error(f"Failed to generate headline: {e}")
        return random.choice([h for h in headlines if h != old_headline] or headlines)

# تجديد التوكن
def refresh_access_token():
    """Refresh Snapchat OAuth access token."""
    try:
        url = "https://accounts.snapchat.com/login/oauth2/access_token"
        data = {
            "client_id": os.getenv("SNAPCHAT_CLIENT_ID"),
            "client_secret": os.getenv("SNAPCHAT_CLIENT_SECRET"),
            "grant_type": "refresh_token",
            "refresh_token": os.getenv("SNAPCHAT_REFRESH_TOKEN")
        }
        response = session.post(url, data=data)
        response.raise_for_status()
        access_token = response.json().get("access_token")
        logging.info("Access token refreshed successfully")
        return access_token
    except requests.RequestException as e:
        logging.error(f"Failed to refresh access token: {e}")
        if e.response is not None:
            logging.error(f"Response content: {e.response.text}")
        raise

# جلب الإعلانات النشطة المرفوضة
def get_active_ads(access_token, ad_account_id):
    """Fetch ads with status: ACTIVE and review_status: REJECTED."""
    try:
        url = f"https://adsapi.snapchat.com/v1/adaccounts/{ad_account_id}/ads"
        headers = {"Authorization": f"Bearer {access_token}"}
        params = {"status": "ACTIVE"}  # تصفية صارمة في طلب API
        response = session.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        ads = data.get("ads", [])
        logging.info(f"Fetched {len(ads)} ads with status=ACTIVE")

        valid_ads = []
        for ad_entry in ads:
            ad = ad_entry.get("ad", {})
            creative_id = ad.get("creative_id")
            status = ad.get("status")
            review_status = ad.get("review_status", "N/A")
            if not creative_id:
                continue  # تجاهل الإعلانات بدون creative_id
            if status != "ACTIVE" or review_status != "REJECTED":
                continue  # تجاهل الإعلانات غير المطابقة دون تسجيل
            valid_ads.append(ad)
            logging.info(f"Selected ad: ID={ad.get('id', 'N/A')}, Creative ID={creative_id}, Status=ACTIVE, Review Status=REJECTED")

        logging.info(f"Valid ads with status=ACTIVE and review_status=REJECTED: {len(valid_ads)}")
        return {ad["creative_id"]: ad for ad in valid_ads}
    except requests.RequestException as e:
        logging.error(f"Failed to fetch active ads: {e}")
        if e.response is not None:
            logging.error(f"Response content: {e.response.text}")
        return {}

# جلب بيانات الإبداعات وإعدادها للتحديث
def get_rejected_creatives(access_token, ad_account_id):
    """Fetch creatives linked to active and rejected ads."""
    try:
        active_ads = get_active_ads(access_token, ad_account_id)
        if not active_ads:
            logging.info("No active ads with review_status=REJECTED found")
            return []

        creatives = []
        for creative_id, ad in active_ads.items():
            # جلب تفاصيل الإبداع
            url = f"https://adsapi.snapchat.com/v1/creatives/{creative_id}"
            headers = {"Authorization": f"Bearer {access_token}"}
            try:
                response = session.get(url, headers=headers)
                response.raise_for_status()
                data = response.json()
                creative = data.get("creatives", [{}])[0].get("creative", {})
                creative_id = creative.get("id")
                if not creative_id:
                    logging.warning(f"Creative without valid ID skipped: {creative_id}")
                    continue
                # إضافة الحقول الإلزامية
                creative["top_snap_media_id"] = creative.get("top_snap_media_id") or os.getenv("SNAPCHAT_TOP_SNAP_MEDIA_ID", "")
                creative["web_view_url"] = creative.get("web_view_properties", {}).get("url") or os.getenv("SNAPCHAT_WEBVIEW_URL", "")
                creative["name"] = creative.get("name") or f"Creative_{creative_id[:8]}"
                creative["call_to_action"] = creative.get("call_to_action") or "LEARN_MORE"
                creative["top_snap_crop_position"] = creative.get("top_snap_crop_position") or "MIDDLE"
                creative["shareable"] = creative.get("shareable", True)
                creative["type"] = creative.get("type") or "WEB_VIEW"
                creative["ad_product"] = creative.get("ad_product") or "SNAP_AD"
                creative["profile_id"] = creative.get("profile_properties", {}).get("profile_id") or os.getenv("SNAPCHAT_PROFILE_ID", "")
                creative["block_preload"] = creative.get("web_view_properties", {}).get("block_preload", True)

                # التحقق من الحقول الإلزامية
                required_fields = ["top_snap_media_id", "web_view_url", "profile_id"]
                missing_fields = [field for field in required_fields if not creative.get(field)]
                if missing_fields:
                    logging.warning(f"Creative {creative_id} skipped due to missing fields: {', '.join(missing_fields)}")
                    continue

                creatives.append({"creative": creative})
                logging.info(f"Prepared creative for update: ID={creative_id}, Associated Ad ID={ad.get('id', 'N/A')}")
            except requests.RequestException as e:
                logging.error(f"Failed to fetch creative {creative_id}: {e}")
                if e.response is not None:
                    logging.error(f"Response content: {e.response.text}")
                continue

        logging.info(f"Prepared {len(creatives)} creatives for headline update")
        return creatives
    except Exception as e:
        logging.error(f"Failed to fetch creatives: {e}")
        return []

# تحديث العنوان
def update_creative_headline(access_token, creative_id, old_headline, creative):
    """Update the headline of a creative linked to an active and rejected ad."""
    try:
        new_headline = generate_new_headline(creative_id, old_headline)
        ad_account_id = os.getenv("SNAPCHAT_AD_ACCOUNTS_ID")
        url = f"https://adsapi.snapchat.com/v1/adaccounts/{ad_account_id}/creatives"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        required_fields = ["web_view_url", "top_snap_media_id", "profile_id"]
        missing_fields = [field for field in required_fields if not creative.get(field)]
        if missing_fields:
            error_message = f"Missing required fields: {', '.join(missing_fields)}"
            logging.error(f"Failed to update creative {creative_id}: {error_message}")
            return "FAILED", new_headline, error_message

        payload = {
            "creatives": [
                {
                    "ad_account_id": ad_account_id,
                    "id": creative_id,
                    "headline": new_headline,
                    "web_view_properties": {
                        "url": creative.get("web_view_url"),
                        "block_preload": creative.get("block_preload")
                    },
                    "type": creative.get("type"),
                    "ad_product": creative.get("ad_product"),
                    "top_snap_media_id": creative.get("top_snap_media_id"),
                    "top_snap_crop_position": creative.get("top_snap_crop_position"),
                    "name": creative.get("name"),
                    "call_to_action": creative.get("call_to_action"),
                    "shareable": creative.get("shareable"),
                    "profile_properties": {
                        "profile_id": creative.get("profile_id")
                    }
                }
            ]
        }
        response = session.put(url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        request_status = data.get("request_status", "UNKNOWN")
        if request_status == "SUCCESS":
            logging.info(f"Updated creative {creative_id} with new headline: {new_headline}")
            return "SUCCESS", new_headline, None
        else:
            error_message = data.get("creatives", [{}])[0].get("sub_request_error_reason", f"Unexpected response: {json.dumps(data)}")
            logging.error(f"Failed to update creative {creative_id}: {error_message}")
            logging.error(f"Response content: {response.text}")
            return "FAILED", new_headline, error_message
    except requests.RequestException as e:
        error_message = str(e)
        logging.error(f"Failed to update creative {creative_id}: {e}")
        if e.response is not None:
            logging.error(f"Response content: {e.response.text}")
            error_message += f" | Response: {e.response.text}"
        return "FAILED", new_headline, error_message

# الدالة الرئيسية
def run_bot():
    """Main function to run the Snapchat Ads bot."""
    try:
        init_db()
        access_token = refresh_access_token()
        ad_account_id = os.getenv("SNAPCHAT_AD_ACCOUNTS_ID")
        creatives = get_rejected_creatives(access_token, ad_account_id)

        if not creatives:
            logging.info("No creatives found for active ads with review_status=REJECTED")
            return

        updates_count = 0
        for item in creatives:  # معالجة جميع الإبداعات دون حد أقصى
            creative = item.get("creative", {})
            creative_id = creative.get("id")
            old_headline = creative.get("headline", creative.get("name", "Unknown"))
            status, new_headline, error_message = update_creative_headline(access_token, creative_id, old_headline, creative)
            log_update(creative_id, old_headline, new_headline, status, error_message)
            updates_count += 1
            logging.info(f"Completed update {updates_count}")
            time.sleep(5)  # تأخير لتجنب قيود API
        logging.info(f"Total updates in this cycle: {updates_count}")
    except Exception as e:
        logging.error(f"Bot execution failed: {e}")

if __name__ == "__main__":
    logging.info("Snapchat Ads bot started, running continuously...")
    while True:
        run_bot()
        logging.info("Waiting for next execution...")
        time.sleep(3600)