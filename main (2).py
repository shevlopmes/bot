import logging
import asyncio
import random
import requests
import aiohttp
# Removed `import sqlite3`
import os
from datetime import datetime, timedelta
from datetime import time as dt_time
import time
from typing import Dict, Any, List
import pytz

import mysql.connector
from mysql.connector import Error

from dotenv import load_dotenv
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup
)
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler,
    JobQueue
)

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
UNSPLASH_ACCESS_KEY = os.getenv("UNSPLASH_ACCESS_KEY")
BOT_OWNER_ID = int(os.getenv("BOT_OWNER_ID"))
BOT_OWNER_ID2 = int(os.getenv("BOT_OWNER_ID2"))
BOT_OWNER_ID3 = int(os.getenv("BOT_OWNER_ID3"))

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)

cyprus_tz = pytz.timezone("Asia/Nicosia")

# Example categories...
wide_categories = {
    "Nature": ["Mountains", "Forests", "Beaches", "Sunsets", "Rivers", "Waterfalls", "Deserts", "Caves"],
    "Space": ["Galaxies", "Planets", "Nebulae", "Stars", "Black Holes"],
    "Animals": ["Wildlife animals", "Pets", "Birds", "Reptiles", "Cats", "Dogs"],
    "Abstract": ["Fractals", "Geometric", "Minimalist", "3D", "Textures", "Surreal"],
    "Cities": ["Skylines", "Bridges", "Streets", "Landmarks", "Nightscapes", "Futuristic Cities"],
    "Fantasy": ["Dragons", "Magical Landscapes", "Fairy Tales", "Fantasy Art"],
    "Technology": ["Cyberpunk", "Futuristic", "AI & Robotics", "Gadgets"],
    "Cars & Vehicles": ["Sports Cars", "Motorcycles", "Classic Cars", "Airplanes", "Trains", "Boats"],
    "Seasons": ["Spring", "Summer", "Autumn", "Winter"],
    "Dark & Gothic": ["Dark Aesthetic", "Horror", "Gothic Art", "Skulls", "Vampires"],
}
narrow_categories = ["Nature", "Abstract", "Animals", "Space", "Cities", "Fantasy", "Technology"]


def get_connection():
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME
        )
        return conn
    except Error as e:
        logger.error(f"Error connecting to MySQL: {e}")
        raise


def init_db():
    try:
        conn = get_connection()
        c = conn.cursor()

        c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            user_group VARCHAR(50) NOT NULL,
            wallpapers_used INT NOT NULL DEFAULT 0,
            wallpapers_received INT NOT NULL DEFAULT 0,
            chosen_category VARCHAR(255),
            last_category_click VARCHAR(50)
        )
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS images (
            id INT PRIMARY KEY AUTO_INCREMENT,
            category_key VARCHAR(255) NOT NULL,
            image_id VARCHAR(100) NOT NULL,
            image_url VARCHAR(255) NOT NULL
        )
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS user_images (
            id INT PRIMARY KEY AUTO_INCREMENT,
            user_id BIGINT NOT NULL,
            image_id VARCHAR(100) NOT NULL,
            UNIQUE KEY unique_user_image (user_id, image_id)
        )
        """)

        conn.commit()
        c.close()
        conn.close()
        logger.info("Database initialised (MySQL).")
    except Exception as e:
        logger.error(f"init_db error: {e}")
        raise


def get_or_create_user(user_id: int) -> Dict[str, Any]:
    conn = get_connection()
    c = None
    try:
        c = conn.cursor(dictionary=True)
        c.execute("""
            SELECT user_id, user_group, wallpapers_used, wallpapers_received, chosen_category, last_category_click
            FROM users
            WHERE user_id = %s
        """, (user_id,))
        row = c.fetchone()
        if row:
            return {
                "user_id": row["user_id"],
                "group": row["user_group"],
                "wallpapers_used": row["wallpapers_used"],
                "wallpapers_received": row["wallpapers_received"],
                "chosen_category": row["chosen_category"],
                "last_category_click": row["last_category_click"]
            }
        else:
            group = random.choice(["narrow", "wide"])
            c.execute("""
                INSERT INTO users (user_id, user_group)
                VALUES (%s, %s)
            """, (user_id, group))
            conn.commit()
            return {
                "user_id": user_id,
                "group": group,
                "wallpapers_used": 0,
                "wallpapers_received": 0,
                "chosen_category": None,
                "last_category_click": ""
            }
    finally:
        if c is not None:
            c.close()  # Close only if 'c' was successfully created
        conn.close()


def update_user(user: Dict[str, Any]):
    conn = get_connection()
    c = None
    try:
        c = conn.cursor()
        c.execute("""
            UPDATE users
            SET user_group = %s,
                wallpapers_used = %s,
                wallpapers_received = %s,
                chosen_category = %s
            WHERE user_id = %s
        """, (
            user["group"],
            user["wallpapers_used"],
            user["wallpapers_received"],
            user["chosen_category"],
            user["user_id"]
        ))
        conn.commit()
    finally:
        if c is not None:
            c.close()  # Close only if 'c' was successfully created
        conn.close()


def fetch_images_from_db(category_key: str, user_id: int) -> List[Dict[str, str]]:
    conn = get_connection()
    c = None
    try:
        c = conn.cursor(dictionary=True)
        c.execute("""
        SELECT i.id, i.image_id, i.image_url
          FROM images i
     LEFT JOIN user_images ui
            ON i.image_id = ui.image_id
           AND ui.user_id = %s
         WHERE i.category_key = %s
           AND ui.image_id IS NULL
        """, (user_id, category_key))
        rows = c.fetchall()
        return [{
            "db_id": r["id"],
            "image_id": r["image_id"],
            "image_url": r["image_url"]
        } for r in rows]
    finally:
        if c is not None:
            c.close()  # Close only if 'c' was successfully created
        conn.close()


async def add_images_to_db(category_key: str, images: List[Dict[str, str]]):
    conn = get_connection()
    c = await asyncio.to_thread(conn.cursor)
    try:
        for img in images:
            c.execute("""
                    INSERT INTO images (category_key, image_id, image_url)
                    VALUES (%s, %s, %s)
                """, (category_key, img["id"], img["url"]))
        await asyncio.to_thread(conn.commit)
    finally:
        if c is not None:
            c.close()  # Close only if 'c' was successfully created
        conn.close()


def mark_image_as_used(user_id: int, image_id: str):
    conn = get_connection()
    c = None
    try:
        c = conn.cursor()
        c.execute("""
            INSERT IGNORE INTO user_images (user_id, image_id)
            VALUES (%s, %s)
        """, (user_id, image_id))
        conn.commit()
    finally:
        if c is not None:
            c.close()  # Close only if 'c' was successfully created
        conn.close()


def check_category_limit(user: Dict[str, Any]) -> bool:
    if user["last_category_click"]:
        last_click = datetime.fromisoformat(user["last_category_click"])
        if datetime.now() - last_click < timedelta(hours=12):
            return False
    return True


def update_category_click(user_id: int):
    conn = get_connection()
    c = None
    try:
        c = conn.cursor()
        c.execute("""
            UPDATE users 
               SET last_category_click = %s 
             WHERE user_id = %s
        """, (datetime.now().isoformat(), user_id))
        conn.commit()
    finally:
        if c is not None:
            c.close()  # Close only if 'c' was successfully created
        conn.close()


# -------------------------
# FETCH FROM UNSPLASH
# -------------------------
async def fetch_images_from_unsplash(query: str, count: int = 5) -> List[Dict[str, str]]:
    logger.warning("Fetching from unsplash")
    url = "https://api.unsplash.com/photos/random"
    params = {
        "query": query,
        "client_id": UNSPLASH_ACCESS_KEY,
        "count": count,
        "orientation": "portrait"
    }
    results = []
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=10) as resp:
                if resp.status_code == 200:
                    data = resp.json()
                    for item in data:
                        results.append({
                            "id": item["id"],
                            "url": item["urls"]["regular"]
                        })
                elif resp.status_code == 403:
                    logger.error(f"Limit is exceeded! Unsplash returned {resp.status_code}: {resp.text}")
                else:
                    logger.error(f"Unsplash returned {resp.status_code}: {resp.text}")
    except Exception as e:
        logger.error(f"Error fetching from Unsplash: {e}")
    return results


# -------------------------
# BOT HANDLERS
# -------------------------
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    logger.warning(f"User {user_id} started")
    user = get_or_create_user(user_id)

    await update.message.reply_text(
        "Hello! You will receive a wallpaper every day in the morning. Stay tuned!"
    )


async def wide_category_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    user = get_or_create_user(user_id)
    logger.warning(f"User {user_id} chose wide category")

    _, category = query.data.split(":", 1)  # "cat:Nature"
    subcats = wide_categories.get(category, [])
    if not subcats:
        if query.message:
            await query.message.reply_text("No subcategories found.")
        else:
            await query.answer("No subcategories found.", show_alert=True)
        return

    keyboard = [
        [InlineKeyboardButton(s, callback_data=f"subcat:{category}:{s}")]
        for s in subcats
    ]

    if query.message:
        await query.message.reply_text(
            f"Subcategories of {category}:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    else:
        await query.answer(f"Subcategories of {category}:", show_alert=True)


async def wide_subcategory_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    user = get_or_create_user(user_id)
    logger.warning(f"User {user_id} chose wide subcategory")

    if not check_category_limit(user):
        await context.bot.send_message(chat_id=user_id, text="You can get only one wallpaper a day.")
        return

    _, main_cat, subcat = query.data.split(":", 2)  # e.g. "subcat:Nature:Mountains"
    category_key = f"{main_cat}:{subcat}"
    user["chosen_category"] = category_key
    update_category_click(user_id)
    update_user(user)

    await send_wallpaper_to_user(user_id, category_key, context)


async def narrow_category_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    user = get_or_create_user(user_id)
    logger.warning(f"User {user_id} chose narrow category")

    if not check_category_limit(user):
        await context.bot.send_message(chat_id=user_id, text="You can only get one wallpaper a day.")
        return

    _, category = query.data.split(":", 1)
    category_key = category
    user["chosen_category"] = category_key
    update_user(user)
    update_category_click(user_id)

    await send_wallpaper_to_user(user_id, category_key, context)


async def send_wallpaper_to_user(user_id: int, category_key: str, context: ContextTypes.DEFAULT_TYPE):
    # 1) Check DB for unused images in the requested category
    logger.warning(f"Trying to  send wallpapers for user {user_id}")
    images = fetch_images_from_db(category_key, user_id)
    if not images:
        # 2) If none in cache, fetch from Unsplash
        new_images = await fetch_images_from_unsplash(category_key, count=5)
        if new_images:
            await add_images_to_db(category_key, new_images)
            # Recheck the DB
            images = fetch_images_from_db(category_key, user_id)

    if not images:
        await context.bot.send_message(
            chat_id=user_id,
            text=f"No new wallpapers for {category_key}, sorry."
        )
        return

    # Pick the first one
    img = images[0]
    image_id = img["image_id"]
    image_url = img["image_url"]

    # Send to user
    try:
        await context.bot.send_photo(chat_id=user_id, photo=image_url)
        await context.bot.send_document(chat_id=user_id, document=image_url)

        # Mark the user as having received this image
        mark_image_as_used(user_id, image_id)

        # Update stats
        user = get_or_create_user(user_id)
        user["wallpapers_received"] += 1
        update_user(user)

    except Exception as e:
        logger.error(f"Error sending image to user {user_id}: {e}")
        await context.bot.send_message(chat_id=user_id, text="Error sending wallpaper, sorry.")


# -------------------------------------------------------
# 3) Nightly Prefetch Job
# -------------------------------------------------------
async def nightly_prefetch(context: ContextTypes.DEFAULT_TYPE):
    """
    This job runs once per night, fetching new images for each category/subcategory.
    We respect ~50 requests/hour. If we hit 48 requests in the current hour,
    we sleep for 1 hour (blocking approach).

    Order of fetching:
      1) Narrow categories (by category name)
      2) Wide categories (by subcategory name)

    Each category or subcategory => 1 request => fetch 5 images from Unsplash.
    """
    logger.warning("Starting nightly prefetch...")

    requests_this_hour = 0
    hour_start = datetime.now()

    async def check_rate_limit(requests_this_hour, hour_start):
        # If we've made 45 requests in this hour, sleep for an hour
        if requests_this_hour >= 45:
            logger.warning("Hit 45 requests this hour, sleeping for 1 hour to respect rate limit...")
            await asyncio.sleep(3600)
            return 0, datetime.now()
        elif (datetime.now() - hour_start) > timedelta(hours=1):
            return 0, datetime.now()  # Reset after an hour
        return requests_this_hour, hour_start

    # 1) Prefetch for NARROW categories (just the category name)
    for cat in narrow_categories:
        requests_this_hour, hour_start = await check_rate_limit(requests_this_hour, hour_start)
        logger.warning(f"Fetching from Unsplash for narrow category: {cat}")
        new_imgs = await fetch_images_from_unsplash(cat, count=5)
        requests_this_hour += 1  # We made one request to Unsplash
        if new_imgs:
            await add_images_to_db(cat, new_imgs)

    # 2) Prefetch for WIDE subcategories
    for main_cat, subcats in wide_categories.items():
        for subcat in subcats:
            await check_rate_limit()
            cat_key = f"{main_cat}:{subcat}"
            logger.warning(f"Fetching from Unsplash for wide subcategory: {cat_key}")
            new_imgs = await fetch_images_from_unsplash(subcat, count=5)
            requests_this_hour += 1
            if new_imgs:
                await add_images_to_db(cat_key, new_imgs)

    logger.warning("Nightly prefetch complete!")


# -------------------------
# DAILY JOB (MORNING DISTRIBUTION)
# -------------------------
async def morning_wallpaper_distribution(context: ContextTypes.DEFAULT_TYPE):
    """Sends a category selection message to all users in the morning."""
    logger.warning("Running morning wallpaper distribution...")
    bot = context.bot

    conn = get_connection()
    c = None
    try:
        c = conn.cursor(dictionary=True)
        c.execute("SELECT user_id, user_group FROM users")
        users = c.fetchall()

    except Exception as e:
        logger.error(f"Database error in morning_wallpaper_distribution: {e}")
        return
    finally:
        if c is not None:
            c.close()  # Close only if 'c' was successfully created
        conn.close()

    tasks = []

    for user in users:
        user_id, group = user["user_id"], user["user_group"]
        try:
            if group == "wide":
                keyboard = [[InlineKeyboardButton(cat, callback_data=f"cat:{cat}")] for cat in wide_categories.keys()]
            else:
                keyboard = [[InlineKeyboardButton(cat, callback_data=f"narrow_cat:{cat}")] for cat in narrow_categories]

            reply_markup = InlineKeyboardMarkup(keyboard)
            tasks.append(
                bot.send_message(chat_id=user_id, text="Good morning! Choose a category:", reply_markup=reply_markup))
    await asyncio.gather(*tasks)


async def nightly_usage_prompt(context: ContextTypes.DEFAULT_TYPE):
    """Asks users if they used their wallpaper at 22:00."""
    logger.warning("Running nightly usage prompt job...")
    bot = context.bot

    conn = get_connection()
    c = None
    try:
        c = conn.cursor(dictionary=True)
        c.execute("SELECT user_id FROM users WHERE wallpapers_received > 0")
        users = c.fetchall()
    except Exception as e:
        logger.error(f"Database error in nightly_usage_prompt: {e}")
        return
    finally:
        if c is not None:
            c.close()  # Close only if 'c' was successfully created
        conn.close()

    for user in users:
        user_id = user["user_id"]
        try:
            keyboard = [
                [
                    InlineKeyboardButton("Yes", callback_data="used:yes"),
                    InlineKeyboardButton("No", callback_data="used:no"),
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await bot.send_message(
                chat_id=user_id,
                text="Would you set this image as your wallpaper?",
                reply_markup=reply_markup
            )
        except Exception as e:
            logger.error(f"Error prompting user {user_id}: {e}")


async def usage_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the user's response to 'did you use it?'"""
    query = update.callback_query
    user_id = query.from_user.id
    user = get_or_create_user(user_id)
    await query.answer()

    data = query.data  # e.g. "used:yes" or "used:no"
    _, answer = data.split(":")
    if answer == "yes":
        user["wallpapers_used"] += 1
        update_user(user)

    await query.message.reply_text("Thank you for the feedback! Good night!")


async def daily_summary(context: ContextTypes.DEFAULT_TYPE):
    """Gathers usage stats for 'narrow' and 'wide' user groups and sends a summary to the bot owner."""
    logger.warning("Generating daily summary...")
    bot = context.bot

    conn = get_connection()
    c = None
    try:
        c = conn.cursor(dictionary=True)

        # Narrow group statistics
        c.execute("""
            SELECT IFNULL(SUM(wallpapers_used), 0) AS used, IFNULL(SUM(wallpapers_received), 0) AS received
            FROM users WHERE user_group = 'narrow'
        """)
        narrow_stats = c.fetchone()
        narrow_used = narrow_stats["used"]
        narrow_received = narrow_stats["received"]
        narrow_rate = (narrow_used / narrow_received * 100) if narrow_received > 0 else 0

        # Wide group statistics
        c.execute("""
            SELECT IFNULL(SUM(wallpapers_used), 0) AS used, IFNULL(SUM(wallpapers_received), 0) AS received
            FROM users WHERE user_group = 'wide'
        """)
        wide_stats = c.fetchone()
        wide_used = wide_stats["used"]
        wide_received = wide_stats["received"]
        wide_rate = (wide_used / wide_received * 100) if wide_received > 0 else 0

        # Overall statistics
        c.execute("""
            SELECT IFNULL(SUM(wallpapers_used), 0) AS used, IFNULL(SUM(wallpapers_received), 0) AS received
            FROM users
        """)
        total_stats = c.fetchone()
        total_used = total_stats["used"]
        total_received = total_stats["received"]
        total_rate = (total_used / total_received * 100) if total_received > 0 else 0

    except Exception as e:
        logger.error(f"Database error in daily_summary: {e}")
        return
    finally:
        if c is not None:
            c.close()  # Close only if 'c' was successfully created
        conn.close()

    summary_text = (
        "📊 **Daily Summary:**\n\n"
        f"**Narrow Group:**\n"
        f"  📌 Wallpapers Received: {narrow_received}\n"
        f"  ✅ Wallpapers Used: {narrow_used}\n"
        f"  📈 Usage Rate: {narrow_rate:.2f}%\n\n"
        f"**Wide Group:**\n"
        f"  📌 Wallpapers Received: {wide_received}\n"
        f"  ✅ Wallpapers Used: {wide_used}\n"
        f"  📈 Usage Rate: {wide_rate:.2f}%\n\n"
        f"**Overall Statistics:**\n"
        f"  📌 Total Received: {total_received}\n"
        f"  ✅ Total Used: {total_used}\n"
        f"  📈 Overall Usage Rate: {total_rate:.2f}%\n"
    )

    # Send the summary to all bot owners
    for owner_id in [BOT_OWNER_ID, BOT_OWNER_ID2, BOT_OWNER_ID3]:
        try:
            await bot.send_message(chat_id=owner_id, text=summary_text, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Error sending summary to owner {owner_id}: {e}")

    logger.info("Daily summary sent successfully.")


# -------------------------
# Main
# -------------------------
def main():
    # 1) init DB
    init_db()

    # 2) build app
    application = ApplicationBuilder().token(BOT_TOKEN).build()

    # 3) Register command/callback handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CallbackQueryHandler(wide_category_callback, pattern=r"^cat:"))
    application.add_handler(CallbackQueryHandler(wide_subcategory_callback, pattern=r"^subcat:"))

    # For narrow group
    application.add_handler(CallbackQueryHandler(narrow_category_callback, pattern=r"^narrow_cat:"))

    application.add_handler(CallbackQueryHandler(usage_callback, pattern=r"^used:"))

    # 4) schedule jobs

    job_queue: JobQueue = application.job_queue
    job_queue.run_daily(
        morning_wallpaper_distribution,
        time=dt_time(hour=11, minute=0, second=0, tzinfo=cyprus_tz),
        days=(0, 1, 2, 3, 4, 5, 6)
    )

    # Nightly usage prompt at 22:00
    job_queue.run_daily(
        nightly_usage_prompt,
        time=dt_time(hour=22, minute=0, second=0, tzinfo=cyprus_tz),
        days=(0, 1, 2, 3, 4, 5, 6)
    )
    # Daily summary at 23:59 (optional)
    job_queue.run_daily(
        daily_summary,
        time=dt_time(hour=23, minute=0, second=0, tzinfo=cyprus_tz),
        days=(0, 1, 2, 3, 4, 5, 6)
    )

    job_queue.run_daily(
        nightly_prefetch,
        time=dt_time(hour=1, minute=0, second=0, tzinfo=cyprus_tz),
        days=(0, 1, 2, 3, 4, 5, 6)
    )

    application.run_polling()


if __name__ == "__main__":
    main()
