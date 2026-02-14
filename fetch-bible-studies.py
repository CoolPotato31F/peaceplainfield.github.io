# fetch_and_store.py
# pip install requests python-dotenv
"""
Usage:
  export YT_API_KEY="YOUR_KEY"
  python fetch_and_store.py

This will write ./data/videos.json (create the data/ dir if missing).
"""

import os
import time
import json
import requests
from datetime import datetime
from math import ceil

# === CONFIG ===
API_KEY = os.getenv("YT_API_KEY") or "AIzaSyAhavTXE_EbUKi9EKoNA0S8oct4fW983GU"
# replace the playlist ids with yours
PLAYLIST_IDS = [
    'PLbuqbfCDMAE-xp_H2EKYCrwUJ-rKO6fuD', # Revelation
    'PLbuqbfCDMAE_MWpDy1sGpvQ-_z-32ovJ_', # The Unvailed Kingdom
    'PLbuqbfCDMAE_SGPhawImwsR3m1omXAZev', # Luke
    'PLbuqbfCDMAE80fArN-lh00mKxM-xJgghQ', # Kindom of God
    'PLbuqbfCDMAE-DQBK5TvsHGqnP2OTdjqv1', # The Athanasian Creed
    'PLbuqbfCDMAE93NQkUKX8eDgIJDnN9cJWo', # End Times
    'PLbuqbfCDMAE9TCJuil__sUWVaKe4oDWy5', # John
    'PLbuqbfCDMAE-2pk4y8T0O95E5k_qLm-s-', # Unforgivable
    'PLbuqbfCDMAE854bzM7kKsrCkhgISDt10P', # Hosea
    'PLbuqbfCDMAE_bBv_hurIfDoV9Jzj5IIxc', # Peter's Letters
    'PLbuqbfCDMAE9rgAE5lYZ5kOFzBu-deI9s', # Malachi
    'PLbuqbfCDMAE8qySl0084Nckh_4DXQT029', # The Lord's Supper
    'PLbuqbfCDMAE_jf-0YTZ4lg1Yc2YkzaaKh', # Nehemiah
    'PLbuqbfCDMAE-GVM2XrEHSB2h0gZGdbkms', # Augsburg Confession
    'PLbuqbfCDMAE9cdFnbNgEbyrQqHn_TBttX', # Job
    'PLbuqbfCDMAE_TeQsgXR-qBWe0Mex0SyHS', # Song Of Songs
    'PLbuqbfCDMAE_Rb3QiyePAuDx9AVGSJLPl', # Exodus
]

OUT_DIR = "data"
OUT_FILE = os.path.join(OUT_DIR, "bible-studies.json")

# === safety checks ===
if API_KEY == "YOUR_YOUTUBE_API_KEY" or not API_KEY:
    raise SystemExit("Set YT_API_KEY environment variable with a valid YouTube Data API v3 key.")

# === helpers ===
def fetch_json(url, params=None, retries=3, backoff=1.0):
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=15)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            if attempt + 1 == retries:
                raise
            time.sleep(backoff * (2 ** attempt))
    raise RuntimeError("unreachable")

def fetch_playlist_video_ids(playlist_id):
    ids = []
    part = "snippet"
    url = "https://www.googleapis.com/youtube/v3/playlistItems"
    params = {"part": part, "maxResults": 50, "playlistId": playlist_id, "key": API_KEY}
    while True:
        data = fetch_json(url, params=params)
        for item in data.get("items", []):
            vid = item.get("snippet", {}).get("resourceId", {}).get("videoId")
            if vid:
                ids.append(vid)
        page_token = data.get("nextPageToken")
        if page_token:
            params["pageToken"] = page_token
        else:
            break
    return ids

def fetch_videos_details(video_id_list):
    # Accepts up to 50 ids
    url = "https://www.googleapis.com/youtube/v3/videos"
    params = {"part": "snippet,contentDetails", "id": ",".join(video_id_list), "maxResults": 50, "key": API_KEY}
    data = fetch_json(url, params=params)
    out = []
    for item in data.get("items", []):
        snippet = item.get("snippet", {})
        thumbnails = snippet.get("thumbnails", {})
        # pick medium or default
        thumb = thumbnails.get("medium", {}).get("url") or thumbnails.get("default", {}).get("url") or ""
        out.append({
            "id": item.get("id"),
            "title": snippet.get("title", ""),
            "publishedAt": snippet.get("publishedAt"),
            "thumbnail": thumb,
            "description": snippet.get("description", ""),
            # you can add more fields as needed
        })
    return out

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    print(f"[{datetime.utcnow().isoformat()}] Starting fetch for {len(PLAYLIST_IDS)} playlists...")

    # collect all video ids (may be duplicates across playlists)
    all_ids = []
    for pl in PLAYLIST_IDS:
        print(" -> fetching playlist:", pl)
        ids = fetch_playlist_video_ids(pl)
        print(f"    got {len(ids)} items")
        all_ids.extend(ids)

    # dedupe while keeping order of first occurrence (we'll sort by date later)
    unique_ids = list(dict.fromkeys(all_ids))
    print(f"Collected {len(unique_ids)} unique video IDs total.")

    # fetch details in batches of 50
    videos = []
    batch_size = 50
    batches = ceil(len(unique_ids) / batch_size)
    for i in range(batches):
        start = i * batch_size
        end = start + batch_size
        batch = unique_ids[start:end]
        print(f"Fetching details batch {i+1}/{batches} ({len(batch)} ids)...")
        details = fetch_videos_details(batch)
        videos.extend(details)
        time.sleep(0.1)  # small pause to be polite (optional)

    # sort newest first by publishedAt (if missing -> push to end)
    def ts_or_zero(item):
        return datetime.fromisoformat(item["publishedAt"].replace("Z", "+00:00")).timestamp() if item.get("publishedAt") else 0
    videos.sort(key=ts_or_zero, reverse=True)

    # output structure
    payload = {
        "fetchedAt": datetime.utcnow().isoformat() + "Z",
        "count": len(videos),
        "videos": videos
    }

    print(f"Writing {OUT_FILE}...")
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print("Done.")

if __name__ == "__main__":
    main()
