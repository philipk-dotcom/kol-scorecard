"""
KOL 자동 스코어카드 - 플랫폼별 스크래퍼
지원 플랫폼: TikTok, Instagram, YouTube, Twitter/X, Lipscosme
"""
from __future__ import annotations

import re
import json
import time
import asyncio
import statistics
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse

# ──────────────────────────────────────────────────────────────
#  공통 유틸
# ──────────────────────────────────────────────────────────────

HEADERS_CHROME = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja-JP,ja;q=0.9,en-US;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

EMPTY_RESULT = {
    "success": False, "error": "", "platform": "",
    "username": "",
    "avg_views": None, "avg_likes": None, "avg_comments": None,
    "avg_saves": None, "avg_shares": None,
    "post_count": 0, "posts": []
}


def detect_platform(url: str) -> str:
    """URL에서 플랫폼 자동 감지"""
    url = url.strip().lower()
    if "tiktok.com" in url:
        return "TikTok"
    if "instagram.com" in url:
        return "Instagram"
    if "youtube.com" in url or "youtu.be" in url:
        return "YouTube"
    if "twitter.com" in url or "x.com" in url:
        return "Twitter"
    if "lipscosme.com" in url:
        return "Lipscosme"
    return "Unknown"


def extract_username(url: str, platform: str) -> str:
    """URL에서 유저명 추출"""
    url = url.strip().rstrip("/")
    try:
        path = urlparse(url).path  # e.g. /@karaimo_desuu
        parts = [p for p in path.split("/") if p]
        if platform == "TikTok":
            # /@username/video/VIDEOID 또는 /@username
            for part in parts:
                if part.lower() != "video" and not part.isdigit():
                    return part.lstrip("@")
        if platform == "Instagram":
            POST_SEGMENTS = {"p", "reel", "tv", "stories"}
            # /username/reel/SHORTCODE  → 첫 번째 세그먼트가 username
            # /reel/SHORTCODE           → username 없음 → ""
            # /username/                → 첫 번째 세그먼트가 username
            for i, part in enumerate(parts):
                if part in POST_SEGMENTS:
                    if i > 0:
                        return parts[0].lstrip("@")   # /username/reel/...
                    else:
                        return ""   # /reel/SHORTCODE — username 없음
            # 일반 프로필 URL
            for part in parts:
                if part not in POST_SEGMENTS:
                    return part.lstrip("@")
        if platform == "YouTube":
            # /@channel 또는 /channel/UCxxx 또는 /c/name
            for part in parts:
                return part.lstrip("@")
        if platform == "Twitter":
            for part in parts:
                return part.lstrip("@")
        if platform == "Lipscosme":
            # /users/@username
            for i, part in enumerate(parts):
                if part == "users" and i + 1 < len(parts):
                    return parts[i + 1].lstrip("@")
            return parts[-1].lstrip("@") if parts else ""
    except Exception:
        pass
    return url  # fallback: return raw URL


def _is_instagram_post_url(url: str) -> bool:
    """Instagram 개별 게시물 URL 여부 (reel/p/tv)"""
    path = urlparse(url.strip()).path
    parts = [p for p in path.split("/") if p]
    return bool(parts) and parts[0] in ("p", "reel", "tv")


def _safe_int(val) -> int | None:
    """안전하게 정수 변환"""
    if val is None:
        return None
    try:
        s = str(val).replace(",", "").replace(" ", "").strip()
        # 만(万) 단위 처리
        if s.endswith("万"):
            return int(float(s[:-1]) * 10000)
        # k 단위 처리
        if s.lower().endswith("k"):
            return int(float(s[:-1]) * 1000)
        # 숫자만 추출
        s = re.sub(r"[^\d.]", "", s)
        if not s:
            return None
        return int(float(s))
    except Exception:
        return None


def _avg(values: list) -> float | None:
    vals = [v for v in values if v is not None]
    return round(statistics.mean(vals)) if vals else None


# ──────────────────────────────────────────────────────────────
#  TikTok 스크래퍼
# ──────────────────────────────────────────────────────────────

def _parse_num(s) -> int | None:
    """TikTok 숫자 파싱 (10.5K → 10500, 1.2M → 1200000)"""
    if s is None:
        return None
    s = str(s).strip().upper()
    try:
        if s.endswith("K"):
            return int(float(s[:-1]) * 1_000)
        if s.endswith("M"):
            return int(float(s[:-1]) * 1_000_000)
        if s.endswith("B"):
            return int(float(s[:-1]) * 1_000_000_000)
        return int(float(re.sub(r"[^\d.]", "", s)))
    except Exception:
        return None


def scrape_tiktok(username: str, num_posts: int = 12,
                  pinned_ids: list = None,
                  playwright_page=None) -> dict:
    """
    TikTok 프로필 스크래핑.
    playwright_page: Streamlit 앱에서 주입된 Playwright Page 객체 (없으면 requests 시도)
    """
    result = {**EMPTY_RESULT, "platform": "TikTok", "username": username}
    pinned_ids = set(str(p) for p in (pinned_ids or []))

    # ── Playwright 방식 ──
    if playwright_page:
        try:
            page = playwright_page
            page.goto(f"https://www.tiktok.com/@{username}", timeout=20000)
            page.wait_for_selector('[data-e2e="user-post-item"]', timeout=15000)

            # 핀 게시물 ID 수집
            pinned_els = page.query_selector_all('[data-e2e="user-post-item-pinned"]')
            detected_pins = set()
            for el in pinned_els:
                href = el.get_attribute("href") or ""
                m = re.search(r"/video/(\d+)", href)
                if m:
                    detected_pins.add(m.group(1))
            pinned_ids = pinned_ids | detected_pins

            # 일반 게시물 수집
            items = page.query_selector_all('[data-e2e="user-post-item"]')
            posts = []
            for item in items:
                href = item.get_attribute("href") or ""
                m = re.search(r"/video/(\d+)", href)
                vid_id = m.group(1) if m else None
                if vid_id and vid_id in pinned_ids:
                    continue

                # 조회수: 그리드에 표시됨
                view_el = item.query_selector('[data-e2e="video-views"], .video-count, strong')
                views = _parse_num(view_el.inner_text() if view_el else None)

                if vid_id:
                    # 개별 영상 페이지에서 상세 지표 수집
                    try:
                        vid_page = page.context.new_page()
                        vid_page.goto(f"https://www.tiktok.com/@{username}/video/{vid_id}",
                                      timeout=15000)
                        vid_page.wait_for_load_state("domcontentloaded")

                        def _grab(selector):
                            el = vid_page.query_selector(selector)
                            return _parse_num(el.inner_text() if el else None)

                        likes    = _grab('[data-e2e="like-count"]')
                        comments = _grab('[data-e2e="comment-count"]')
                        saves    = _grab('[data-e2e="undefined-count"]')  # 저장(수집)
                        shares   = _grab('[data-e2e="share-count"]')
                        if views is None:
                            vv = vid_page.query_selector('[data-e2e="video-views"]')
                            views = _parse_num(vv.inner_text() if vv else None)
                        vid_page.close()
                    except Exception:
                        likes = comments = saves = shares = None

                    posts.append({
                        "id": vid_id, "views": views, "likes": likes,
                        "comments": comments, "saves": saves, "shares": shares
                    })

                if len(posts) >= num_posts:
                    break

            if not posts:
                result["error"] = "게시물을 찾을 수 없습니다. 로그인 후 다시 시도하세요."
                return result

            result["success"] = True
            result["posts"] = posts
            result["post_count"] = len(posts)
            result["avg_views"]    = _avg([p["views"]    for p in posts])
            result["avg_likes"]    = _avg([p["likes"]    for p in posts])
            result["avg_comments"] = _avg([p["comments"] for p in posts])
            result["avg_saves"]    = _avg([p["saves"]    for p in posts])
            result["avg_shares"]   = _avg([p["shares"]   for p in posts])
            return result

        except Exception as e:
            # Playwright 실패 시 requests 방식으로 fallback
            result["error"] = ""  # 에러 초기화 후 아래 requests 방식 시도
            # (fall through)

    # ── requests 방식 (쿠키 있으면 사용) ──
    try:
        MOBILE_UA = (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 "
            "Mobile/15E148 Safari/604.1"
        )
        sess = requests.Session()
        sess.headers.update({
            **HEADERS_CHROME,
            "User-Agent": MOBILE_UA,
            "Referer": "https://www.tiktok.com/",
        })
        # 세션 쿠키 적용 (클라우드 로그인용)
        session_cookies = _get_session_cookies()
        if session_cookies.get("tiktok"):
            sess.cookies.set("sessionid", session_cookies["tiktok"], domain=".tiktok.com")
        resp = sess.get(f"https://www.tiktok.com/@{username}", timeout=20)

        items = []

        # ① UNIVERSAL_DATA_FOR_REHYDRATION (최신 TikTok 포맷)
        m = re.search(
            r'id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>([\s\S]*?)</script>',
            resp.text
        )
        if m and not items:
            try:
                d = json.loads(m.group(1))
                items = (d.get("__DEFAULT_SCOPE__", {})
                          .get("webapp.video-list", {})
                          .get("itemList", []))
            except Exception:
                pass

        # ② SIGI_STATE
        if not items:
            m = re.search(r'id="SIGI_STATE"[^>]*>([\s\S]*?)</script>', resp.text)
            if m:
                try:
                    d = json.loads(m.group(1))
                    module = d.get("ItemModule", {})
                    items = list(module.values()) if module else []
                except Exception:
                    pass

        # ③ __NEXT_DATA__ (구버전 포맷)
        if not items:
            m = re.search(r'id="__NEXT_DATA__"[^>]*>([\s\S]*?)</script>', resp.text)
            if m:
                try:
                    d = json.loads(m.group(1))
                    pp = d.get("props", {}).get("pageProps", {})
                    items = pp.get("itemList") or pp.get("videoFeed") or []
                except Exception:
                    pass

        if not items:
            result["error"] = "데이터 추출 실패 — 수동으로 지표를 입력해주세요."
            return result

        def _extract_stats(item):
            s = item.get("stats") or item.get("statsV2") or {}
            return {
                "views":    s.get("playCount") or s.get("vvCount"),
                "likes":    s.get("diggCount"),
                "comments": s.get("commentCount"),
                "saves":    s.get("collectCount"),
                "shares":   s.get("shareCount"),
            }

        posts = []
        for item in items:
            vid_id = str(item.get("id", ""))
            if vid_id in pinned_ids:
                continue
            st = _extract_stats(item)
            posts.append({"id": vid_id, **st})
            if len(posts) >= num_posts:
                break

        if not posts:
            result["error"] = "게시물 없음 — 비공개 계정이거나 수동 입력이 필요합니다."
            return result

        result["success"] = True
        result["posts"] = posts
        result["post_count"] = len(posts)
        result["avg_views"]    = _avg([p["views"]    for p in posts])
        result["avg_likes"]    = _avg([p["likes"]    for p in posts])
        result["avg_comments"] = _avg([p["comments"] for p in posts])
        result["avg_saves"]    = _avg([p["saves"]    for p in posts])
        result["avg_shares"]   = _avg([p["shares"]   for p in posts])
        return result

    except Exception as e:
        result["error"] = f"requests 오류: {e}"
        return result


# ──────────────────────────────────────────────────────────────
#  Instagram 스크래퍼
# ──────────────────────────────────────────────────────────────

def scrape_instagram(username: str, num_posts: int = 12,
                     playwright_page=None) -> dict:
    result = {**EMPTY_RESULT, "platform": "Instagram", "username": username}

    if playwright_page:
        try:
            page = playwright_page
            page.goto(f"https://www.instagram.com/{username}/", timeout=20000)
            page.wait_for_load_state("domcontentloaded")

            # 포스트 썸네일 목록
            page.wait_for_selector("article a", timeout=10000)
            post_links = page.eval_on_selector_all(
                "article a",
                "els => els.map(e => e.href)"
            )
            post_links = list(dict.fromkeys(post_links))[:num_posts * 2]  # dedup

            posts = []
            for link in post_links:
                if len(posts) >= num_posts:
                    break
                if "/p/" not in link and "/reel/" not in link:
                    continue
                try:
                    post_page = page.context.new_page()
                    post_page.goto(link, timeout=15000)
                    post_page.wait_for_load_state("domcontentloaded")

                    text = post_page.content()
                    # 좋아요 수 추출
                    like_m = re.search(r'"like_count":(\d+)', text)
                    com_m  = re.search(r'"comment_count":(\d+)', text)
                    likes    = int(like_m.group(1)) if like_m else None
                    comments = int(com_m.group(1))  if com_m  else None

                    # 저장 수는 Instagram이 공개하지 않으므로 None
                    posts.append({
                        "id": link, "views": None, "likes": likes,
                        "comments": comments, "saves": None, "shares": None
                    })
                    post_page.close()
                except Exception:
                    pass

            if not posts:
                result["error"] = "게시물 없음 (비공개 계정이거나 로그인 필요)"
                return result

            result["success"] = True
            result["posts"] = posts
            result["post_count"] = len(posts)
            result["avg_views"]    = None  # Instagram은 조회수 비공개
            result["avg_likes"]    = _avg([p["likes"]    for p in posts])
            result["avg_comments"] = _avg([p["comments"] for p in posts])
            result["avg_saves"]    = None
            result["avg_shares"]   = None
            return result

        except Exception as e:
            # Playwright 실패 → requests fallback으로 계속
            result["error"] = ""
            # (fall through)

    # requests fallback (쿠키 있으면 사용)
    try:
        sess = requests.Session()
        sess.headers.update(HEADERS_CHROME)
        # 세션 쿠키 적용 (클라우드 로그인용)
        session_cookies = _get_session_cookies()
        if session_cookies.get("instagram"):
            sess.cookies.set("sessionid", session_cookies["instagram"], domain=".instagram.com")
        # Instagram public JSON endpoint
        resp = sess.get(
            f"https://www.instagram.com/api/v1/users/web_profile_info/?username={username}",
            headers={**HEADERS_CHROME, "X-IG-App-ID": "936619743392459"},
            timeout=15
        )
        if resp.status_code != 200:
            if session_cookies.get("instagram"):
                result["error"] = (
                    "📋 Instagram 세션이 만료되었습니다.\n"
                    "사이드바에서 새 sessionid를 입력해주세요."
                )
            else:
                result["error"] = (
                    "📋 Instagram은 로그인이 필요합니다.\n"
                    "사이드바에서 Instagram 로그인 → sessionid를 입력해주세요.\n"
                    "또는 STEP 3 표에서 좋아요·댓글 수를 직접 입력하세요."
                )
            return result

        data = resp.json()
        edges = (
            data.get("data", {})
                .get("user", {})
                .get("edge_owner_to_timeline_media", {})
                .get("edges", [])
        )
        posts = []
        for edge in edges[:num_posts]:
            node = edge.get("node", {})
            posts.append({
                "id": node.get("shortcode", ""),
                "views":    node.get("video_view_count"),
                "likes":    node.get("edge_liked_by", {}).get("count"),
                "comments": node.get("edge_media_to_comment", {}).get("count"),
                "saves":    None,
                "shares":   None,
            })

        if not posts:
            result["error"] = "게시물 없음 (비공개 또는 로그인 필요)"
            return result

        result["success"] = True
        result["posts"] = posts
        result["post_count"] = len(posts)
        result["avg_views"]    = _avg([p["views"]    for p in posts])
        result["avg_likes"]    = _avg([p["likes"]    for p in posts])
        result["avg_comments"] = _avg([p["comments"] for p in posts])
        result["avg_saves"]    = None
        result["avg_shares"]   = None
        return result

    except Exception as e:
        result["error"] = f"오류: {e}"
        return result


# ──────────────────────────────────────────────────────────────
#  YouTube 스크래퍼
# ──────────────────────────────────────────────────────────────

def scrape_youtube(channel_url: str, num_posts: int = 12,
                   playwright_page=None) -> dict:
    username = extract_username(channel_url, "YouTube")
    result = {**EMPTY_RESULT, "platform": "YouTube", "username": username}

    # yt-dlp 방식 (가장 안정적)
    try:
        import subprocess, sys
        cmd = [
            sys.executable, "-m", "yt_dlp",
            "--flat-playlist",
            "--dump-json",
            f"--playlist-items", f"1:{num_posts}",
            "--no-warnings",
            f"{channel_url.rstrip('/')}/videos"
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        lines = [l for l in proc.stdout.strip().split("\n") if l.strip()]
        if not lines:
            raise ValueError("yt-dlp 출력 없음")

        # 각 영상의 상세 정보 수집
        posts = []
        for line in lines[:num_posts]:
            try:
                info = json.loads(line)
                vid_id = info.get("id", "")
                # 개별 영상 상세 정보 가져오기
                detail_cmd = [
                    sys.executable, "-m", "yt_dlp",
                    "--dump-json", "--no-warnings",
                    "--skip-download",
                    f"https://www.youtube.com/watch?v={vid_id}"
                ]
                detail_proc = subprocess.run(
                    detail_cmd, capture_output=True, text=True, timeout=20
                )
                if detail_proc.returncode == 0 and detail_proc.stdout.strip():
                    detail = json.loads(detail_proc.stdout.strip())
                    posts.append({
                        "id":       vid_id,
                        "views":    detail.get("view_count"),
                        "likes":    detail.get("like_count"),
                        "comments": detail.get("comment_count"),
                        "saves":    None,
                        "shares":   None,
                    })
                else:
                    posts.append({
                        "id": vid_id,
                        "views": info.get("view_count"),
                        "likes": None, "comments": None,
                        "saves": None, "shares": None,
                    })
            except Exception:
                continue

        if not posts:
            result["error"] = "게시물 없음"
            return result

        result["success"] = True
        result["posts"] = posts
        result["post_count"] = len(posts)
        result["avg_views"]    = _avg([p["views"]    for p in posts])
        result["avg_likes"]    = _avg([p["likes"]    for p in posts])
        result["avg_comments"] = _avg([p["comments"] for p in posts])
        result["avg_saves"]    = None
        result["avg_shares"]   = None
        return result

    except Exception as e:
        result["error"] = f"yt-dlp 오류: {e} (pip install yt-dlp 로 설치하세요)"
        return result


# ──────────────────────────────────────────────────────────────
#  Twitter/X 스크래퍼
# ──────────────────────────────────────────────────────────────

def scrape_twitter(username: str, num_posts: int = 12,
                   playwright_page=None) -> dict:
    result = {**EMPTY_RESULT, "platform": "Twitter", "username": username}

    if not playwright_page:
        result["error"] = "Twitter는 로그인이 필요합니다. 브라우저 로그인 후 재시도하세요."
        return result

    try:
        page = playwright_page
        page.goto(f"https://x.com/{username}", timeout=20000)
        page.wait_for_selector('[data-testid="tweet"]', timeout=15000)

        # 스크롤로 트윗 더 로드
        for _ in range(3):
            page.keyboard.press("End")
            time.sleep(1)

        tweets = page.query_selector_all('[data-testid="tweet"]')
        posts = []
        for tweet in tweets[:num_posts * 2]:
            try:
                # 조회수
                view_el   = tweet.query_selector('[data-testid="app-text-transition-container"]')
                like_el   = tweet.query_selector('[data-testid="like"]')
                reply_el  = tweet.query_selector('[data-testid="reply"]')
                retweet_el= tweet.query_selector('[data-testid="retweet"]')

                views    = _parse_num(view_el.inner_text()    if view_el    else None)
                likes    = _parse_num(like_el.inner_text()    if like_el    else None)
                comments = _parse_num(reply_el.inner_text()   if reply_el   else None)
                shares   = _parse_num(retweet_el.inner_text() if retweet_el else None)

                posts.append({
                    "id": "", "views": views, "likes": likes,
                    "comments": comments, "saves": None, "shares": shares
                })
                if len(posts) >= num_posts:
                    break
            except Exception:
                continue

        if not posts:
            result["error"] = "트윗 없음 또는 로그인 필요"
            return result

        result["success"] = True
        result["posts"] = posts
        result["post_count"] = len(posts)
        result["avg_views"]    = _avg([p["views"]    for p in posts])
        result["avg_likes"]    = _avg([p["likes"]    for p in posts])
        result["avg_comments"] = _avg([p["comments"] for p in posts])
        result["avg_saves"]    = None
        result["avg_shares"]   = _avg([p["shares"]   for p in posts])
        return result

    except Exception as e:
        result["error"] = f"Playwright 오류: {e}"
        return result


# ──────────────────────────────────────────────────────────────
#  Lipscosme 스크래퍼
# ──────────────────────────────────────────────────────────────

def scrape_lipscosme(username: str, num_posts: int = 12,
                     pinned_ids: list = None) -> dict:
    """Lipscosme 프로필 스크래핑 (requests 기반, 로그인 불필요)"""
    result = {**EMPTY_RESULT, "platform": "Lipscosme", "username": username}
    pinned_ids_set = set(str(p) for p in (pinned_ids or []))

    try:
        sess = requests.Session()
        sess.headers.update(HEADERS_CHROME)

        # 프로필 페이지에서 NEXT_DATA 추출
        profile_url = f"https://lipscosme.com/users/@{username}"
        resp = sess.get(profile_url, timeout=15)
        if resp.status_code != 200:
            result["error"] = f"프로필 페이지 접근 실패: HTTP {resp.status_code}"
            return result

        m = re.search(
            r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
            resp.text, re.DOTALL
        )
        if not m:
            result["error"] = "NEXT_DATA 없음"
            return result

        data = json.loads(m.group(1))
        try:
            page_props = data["props"]["pageProps"]
            posts_data = page_props.get("initialPostsData", {})
            all_posts  = posts_data.get("userPosts", {}).get("posts", [])
            pinned_from_api = posts_data.get("userPosts", {}).get("pinnedPostIds", [])
        except (KeyError, TypeError):
            result["error"] = "페이지 데이터 구조 불일치"
            return result

        # 핀 게시물 ID 합집합
        all_pinned = pinned_ids_set | set(str(p) for p in pinned_from_api)

        # 핀 제외 후 최신 12개
        filtered = [
            p for p in all_posts
            if str(p.get("id", "")) not in all_pinned
        ][:num_posts]

        if not filtered:
            # 두 번째 페이지 시도
            try:
                page2 = page_props.get("initialPostsData", {}) \
                                   .get("userPosts", {}).get("totalPages", 1)
                if page2 > 1:
                    resp2 = sess.get(f"{profile_url}?page=2", timeout=15)
                    m2 = re.search(
                        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
                        resp2.text, re.DOTALL
                    )
                    if m2:
                        data2 = json.loads(m2.group(1))
                        extra = (data2["props"]["pageProps"]
                                       .get("initialPostsData", {})
                                       .get("userPosts", {}).get("posts", []))
                        filtered.extend(extra)
            except Exception:
                pass

        # 각 게시물 상세 지표 수집
        posts = []
        for post in filtered[:num_posts]:
            post_id = str(post.get("id", ""))
            try:
                post_resp = sess.get(
                    f"https://lipscosme.com/posts/{post_id}", timeout=10
                )
                html = post_resp.text

                def _extract(cls):
                    m_ = re.search(
                        rf'class="{cls}"[^>]*>\s*<[^>]*>(\d[\d,]*)', html
                    )
                    if m_:
                        return int(m_.group(1).replace(",", ""))
                    # 직접 텍스트 패턴
                    m2 = re.search(
                        rf'class="{cls}"[^>]*>(\d[\d,]*)', html
                    )
                    return int(m2.group(1).replace(",", "")) if m2 else None

                likes    = _extract("like-count")
                saves    = _extract("clip-count")
                comments = _extract("comment-count")
                posts.append({
                    "id": post_id, "views": None, "likes": likes,
                    "comments": comments, "saves": saves, "shares": None
                })
            except Exception:
                posts.append({
                    "id": post_id, "views": None, "likes": None,
                    "comments": None, "saves": None, "shares": None
                })
            time.sleep(0.3)

        if not posts:
            result["error"] = "게시물 없음"
            return result

        result["success"] = True
        result["posts"] = posts
        result["post_count"] = len(posts)
        result["avg_views"]    = None
        result["avg_likes"]    = _avg([p["likes"]    for p in posts])
        result["avg_comments"] = _avg([p["comments"] for p in posts])
        result["avg_saves"]    = _avg([p["saves"]    for p in posts])
        result["avg_shares"]   = None
        return result

    except Exception as e:
        result["error"] = f"오류: {e}"
        return result


# ──────────────────────────────────────────────────────────────
#  Instagram 개별 게시물 스크래퍼 (reel/p URL 직접 입력 시)
# ──────────────────────────────────────────────────────────────

def _scrape_instagram_single_post(url: str, playwright_page=None) -> dict:
    """
    https://www.instagram.com/reel/SHORTCODE/ 같은 개별 게시물 URL 스크래핑.
    좋아요·댓글 1개 게시물만 반환.
    """
    result = {**EMPTY_RESULT, "platform": "Instagram", "username": url}
    url = url.strip().rstrip("/")

    # Playwright 방식
    if playwright_page:
        try:
            page = playwright_page
            page.goto(url, timeout=20000)
            page.wait_for_load_state("domcontentloaded")
            text = page.content()

            like_m = re.search(r'"like_count":(\d+)', text)
            com_m  = re.search(r'"comment_count":(\d+)', text)
            likes    = int(like_m.group(1)) if like_m else None
            comments = int(com_m.group(1))  if com_m  else None

            # 페이지에서 실제 username 추출 시도
            user_m = re.search(r'"owner":\{"username":"([^"]+)"', text)
            if user_m:
                result["username"] = user_m.group(1)

            posts = [{"id": url, "views": None, "likes": likes,
                      "comments": comments, "saves": None, "shares": None}]
            result["success"] = True
            result["posts"] = posts
            result["post_count"] = 1
            result["avg_likes"]    = likes
            result["avg_comments"] = comments
            return result
        except Exception:
            pass  # fall through

    # requests fallback — 그래프QL 엔드포인트 시도 (쿠키 사용)
    try:
        shortcode = [p for p in urlparse(url).path.split("/") if p][-1]
        sess = requests.Session()
        sess.headers.update(HEADERS_CHROME)
        session_cookies = _get_session_cookies()
        if session_cookies.get("instagram"):
            sess.cookies.set("sessionid", session_cookies["instagram"], domain=".instagram.com")
        api_url = (
            f"https://www.instagram.com/api/graphql?variables="
            f'%7B"shortcode"%3A"{shortcode}"%7D'
            f"&doc_id=8845758582119845"
        )
        resp = sess.get(api_url, timeout=15)
        if resp.ok:
            d = resp.json()
            media = (d.get("data", {})
                      .get("xdt_shortcode_media") or
                     d.get("data", {})
                      .get("shortcode_media") or {})
            likes    = media.get("edge_media_preview_like", {}).get("count")
            comments = media.get("edge_media_to_parent_comment", {}).get("count")
            owner    = media.get("owner", {}).get("username", "")
            if owner:
                result["username"] = owner
            posts = [{"id": shortcode, "views": media.get("video_view_count"),
                      "likes": likes, "comments": comments,
                      "saves": None, "shares": None}]
            result["success"] = True
            result["posts"] = posts
            result["post_count"] = 1
            result["avg_views"]    = media.get("video_view_count")
            result["avg_likes"]    = likes
            result["avg_comments"] = comments
            return result
    except Exception:
        pass

    result["error"] = (
        "📋 Instagram 수집에 실패했습니다.\n"
        "① 사이드바에서 Instagram 로그인 → sessionid 입력\n"
        "② 또는 STEP 3 표에서 좋아요·댓글 수를 직접 입력 후 점수 계산"
    )
    return result


# ──────────────────────────────────────────────────────────────
#  통합 스크래퍼 진입점
# ──────────────────────────────────────────────────────────────

def _get_session_cookies() -> dict:
    """Streamlit session_state에서 쿠키 가져오기 (클라우드용)"""
    cookies = {}
    try:
        import streamlit as st
        if st.session_state.get("tiktok_session_cookie"):
            cookies["tiktok"] = st.session_state["tiktok_session_cookie"]
        if st.session_state.get("instagram_session_cookie"):
            cookies["instagram"] = st.session_state["instagram_session_cookie"]
    except Exception:
        pass
    return cookies


def scrape_kol(
    url: str,
    num_posts: int = 12,
    pinned_ids: list = None,
    playwright_page=None
) -> dict:
    """
    URL을 받아 플랫폼 감지 후 적절한 스크래퍼 호출.
    반환: {success, error, platform, username,
          avg_views, avg_likes, avg_comments, avg_saves, avg_shares, post_count, posts}
    """
    platform = detect_platform(url)
    username = extract_username(url, platform)

    if platform == "TikTok":
        return scrape_tiktok(username, num_posts, pinned_ids, playwright_page)
    if platform == "Instagram":
        if not username or _is_instagram_post_url(url):
            return _scrape_instagram_single_post(url, playwright_page)
        return scrape_instagram(username, num_posts, playwright_page)
    if platform == "YouTube":
        return scrape_youtube(url, num_posts, playwright_page)
    if platform == "Twitter":
        return scrape_twitter(username, num_posts, playwright_page)
    if platform == "Lipscosme":
        return scrape_lipscosme(username, num_posts, pinned_ids)

    return {**EMPTY_RESULT, "platform": platform,
            "error": f"지원하지 않는 플랫폼: {url}"}


# ──────────────────────────────────────────────────────────────
#  F1: 브랜드 키워드 검색 스크래퍼
# ──────────────────────────────────────────────────────────────

SEARCH_RESULT = {
    "success": False, "error": "", "platform": "",
    "brand": "", "posts": [],
}


def search_tiktok_brand(brand: str, max_results: int = 30,
                        playwright_page=None) -> dict:
    """TikTok에서 브랜드명 검색 → 콘텐츠 목록 반환"""
    from urllib.parse import quote
    result = {**SEARCH_RESULT, "platform": "TikTok", "brand": brand}

    # ── 방법 1: Playwright (로그인 세션 사용) ──
    if playwright_page:
        try:
            page = playwright_page
            search_url = f"https://www.tiktok.com/search/video?q={quote(brand)}"
            page.goto(search_url, timeout=25000)
            page.wait_for_selector(
                '[data-e2e="search_video-item"], [data-e2e="search-card-desc"], article',
                timeout=15000
            )

            for _ in range(4):
                page.keyboard.press("End")
                time.sleep(1.5)

            # 여러 셀렉터 시도 (TikTok UI가 자주 변경됨)
            items = page.query_selector_all('[data-e2e="search_video-item"]')
            if not items:
                items = page.query_selector_all('[data-e2e="search-common-link"]')
            if not items:
                items = page.query_selector_all('div[class*="DivItemCard"]')

            posts = []
            for item in items[:max_results]:
                try:
                    link_el = item.query_selector("a")
                    href = link_el.get_attribute("href") if link_el else ""
                    if not href:
                        href = item.get_attribute("href") or ""

                    desc_el = (item.query_selector('[data-e2e="search-card-desc"]')
                               or item.query_selector('span[class*="SpanText"]'))
                    desc = desc_el.inner_text() if desc_el else ""

                    # 조회수 - 여러 셀렉터
                    view_el = (item.query_selector('[data-e2e="video-views"]')
                               or item.query_selector('strong[data-e2e="video-views"]')
                               or item.query_selector('strong'))
                    views = _parse_num(view_el.inner_text() if view_el else None)

                    author_el = (item.query_selector('[data-e2e="search-card-user-unique-id"]')
                                 or item.query_selector('p[data-e2e="search-card-user-unique-id"]'))
                    author = author_el.inner_text().strip() if author_el else ""

                    # username을 URL에서 추출 시도
                    if not author and href:
                        m = re.search(r"@([\w.]+)", href)
                        if m:
                            author = m.group(1)

                    if href:
                        posts.append({
                            "url": href, "kol_name": author, "platform": "TikTok",
                            "description": desc, "views": views,
                            "likes": None, "comments": None, "saves": None,
                            "hashtags": re.findall(r"#[\w\u3000-\u9fff]+", desc),
                        })
                except Exception:
                    continue

            if posts:
                result["success"] = True
                result["posts"] = posts
                return result

        except Exception as e:
            pass  # fallback to requests

    # ── 방법 2: requests (모바일 웹 HTML 파싱, 쿠키 사용) ──
    try:
        from urllib.parse import quote as _q
        MOBILE_UA = (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 "
            "Mobile/15E148 Safari/604.1"
        )
        sess = requests.Session()
        sess.headers.update({
            **HEADERS_CHROME,
            "User-Agent": MOBILE_UA,
            "Referer": "https://www.tiktok.com/",
        })
        # 세션 쿠키 적용
        session_cookies = _get_session_cookies()
        if session_cookies.get("tiktok"):
            sess.cookies.set("sessionid", session_cookies["tiktok"], domain=".tiktok.com")

        resp = sess.get(
            f"https://www.tiktok.com/search/video?q={_q(brand)}",
            timeout=20
        )

        posts = []

        # JSON 데이터 추출 시도
        m = re.search(
            r'id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>([\s\S]*?)</script>',
            resp.text
        )
        if m:
            try:
                d = json.loads(m.group(1))
                items = (d.get("__DEFAULT_SCOPE__", {})
                          .get("webapp.search-list", {})
                          .get("itemList", []))
                if not items:
                    # 다른 경로 시도
                    for key in d.get("__DEFAULT_SCOPE__", {}):
                        if "search" in key.lower():
                            items = d["__DEFAULT_SCOPE__"][key].get("itemList", [])
                            if items:
                                break

                for item in items[:max_results]:
                    stats = item.get("stats") or item.get("statsV2") or {}
                    desc = item.get("desc", "")
                    author = item.get("author", {}).get("uniqueId", "")
                    vid_id = item.get("id", "")
                    posts.append({
                        "url": f"https://www.tiktok.com/@{author}/video/{vid_id}",
                        "kol_name": author, "platform": "TikTok",
                        "description": desc,
                        "views": stats.get("playCount") or stats.get("vvCount"),
                        "likes": stats.get("diggCount"),
                        "comments": stats.get("commentCount"),
                        "saves": stats.get("collectCount"),
                        "hashtags": re.findall(r"#[\w\u3000-\u9fff]+", desc),
                    })
            except Exception:
                pass

        # SIGI_STATE fallback
        if not posts:
            m2 = re.search(r'id="SIGI_STATE"[^>]*>([\s\S]*?)</script>', resp.text)
            if m2:
                try:
                    d2 = json.loads(m2.group(1))
                    module = d2.get("ItemModule", {})
                    for vid_id, item in list(module.items())[:max_results]:
                        stats = item.get("stats", {})
                        desc = item.get("desc", "")
                        author = item.get("author", "")
                        posts.append({
                            "url": f"https://www.tiktok.com/@{author}/video/{vid_id}",
                            "kol_name": author, "platform": "TikTok",
                            "description": desc,
                            "views": _safe_int(stats.get("playCount")),
                            "likes": _safe_int(stats.get("diggCount")),
                            "comments": _safe_int(stats.get("commentCount")),
                            "saves": _safe_int(stats.get("collectCount")),
                            "hashtags": re.findall(r"#[\w\u3000-\u9fff]+", desc),
                        })
                except Exception:
                    pass

        if posts:
            result["success"] = True
            result["posts"] = posts
        else:
            result["error"] = "TikTok 검색 결과 없음. 로그인이 필요할 수 있습니다."
        return result

    except Exception as e:
        result["error"] = f"TikTok 검색 오류: {e}"
        return result


def search_instagram_brand(brand: str, max_results: int = 30,
                           playwright_page=None) -> dict:
    """Instagram에서 해시태그/키워드 검색"""
    result = {**SEARCH_RESULT, "platform": "Instagram", "brand": brand}

    if not playwright_page:
        result["error"] = "Instagram 검색은 로그인이 필요합니다."
        return result

    try:
        page = playwright_page
        tag = brand.replace(" ", "").replace("#", "")
        page.goto(f"https://www.instagram.com/explore/tags/{tag}/", timeout=20000)
        page.wait_for_selector("article a", timeout=15000)

        # 스크롤
        for _ in range(3):
            page.keyboard.press("End")
            time.sleep(1.5)

        links = page.eval_on_selector_all("article a", "els => els.map(e => e.href)")
        links = list(dict.fromkeys(links))[:max_results]

        posts = []
        for link in links:
            if "/p/" not in link and "/reel/" not in link:
                continue
            try:
                post_page = page.context.new_page()
                post_page.goto(link, timeout=15000)
                post_page.wait_for_load_state("domcontentloaded")
                text = post_page.content()

                like_m = re.search(r'"like_count":(\d+)', text)
                com_m = re.search(r'"comment_count":(\d+)', text)
                user_m = re.search(r'"owner":\{"username":"([^"]+)"', text)
                desc_m = re.search(r'"caption":\{"text":"(.*?)"', text)

                author = user_m.group(1) if user_m else ""
                desc = desc_m.group(1) if desc_m else ""

                posts.append({
                    "url": link, "kol_name": author, "platform": "Instagram",
                    "description": desc,
                    "views": None,
                    "likes": int(like_m.group(1)) if like_m else None,
                    "comments": int(com_m.group(1)) if com_m else None,
                    "saves": None,
                    "hashtags": re.findall(r"#[\w\u3000-\u9fff]+", desc),
                })
                post_page.close()
            except Exception:
                pass
            if len(posts) >= max_results:
                break

        if posts:
            result["success"] = True
            result["posts"] = posts
        else:
            result["error"] = "검색 결과 없음"
        return result

    except Exception as e:
        result["error"] = f"Instagram 검색 오류: {e}"
        return result


def search_youtube_brand(brand: str, max_results: int = 30) -> dict:
    """YouTube에서 브랜드 검색 (yt-dlp) — 상세 메트릭 포함"""
    result = {**SEARCH_RESULT, "platform": "YouTube", "brand": brand}

    try:
        import subprocess, sys

        # yt-dlp로 검색 + 상세 정보 (--flat-playlist 없이)
        # 먼저 flat으로 ID 목록 가져오기
        cmd_flat = [
            sys.executable, "-m", "yt_dlp",
            "--flat-playlist", "--dump-json",
            f"--playlist-items", f"1:{max_results}",
            "--no-warnings",
            f"ytsearch{max_results}:{brand}"
        ]
        proc = subprocess.run(cmd_flat, capture_output=True, text=True, timeout=60)
        lines = [l for l in proc.stdout.strip().split("\n") if l.strip()]

        posts = []
        for line in lines[:max_results]:
            try:
                info = json.loads(line)
                vid_id = info.get("id", "")
                title = info.get("title", "")
                channel = info.get("channel", "") or info.get("uploader", "")

                views = info.get("view_count")
                likes = info.get("like_count")
                comments = info.get("comment_count")

                # flat-playlist는 상세가 부족하면 개별 조회
                if views is None or likes is None:
                    try:
                        detail_cmd = [
                            sys.executable, "-m", "yt_dlp",
                            "--dump-json", "--no-warnings", "--skip-download",
                            f"https://www.youtube.com/watch?v={vid_id}"
                        ]
                        dp = subprocess.run(detail_cmd, capture_output=True, text=True, timeout=20)
                        if dp.returncode == 0 and dp.stdout.strip():
                            detail = json.loads(dp.stdout.strip())
                            views = detail.get("view_count") or views
                            likes = detail.get("like_count") or likes
                            comments = detail.get("comment_count") or comments
                            channel = detail.get("channel") or detail.get("uploader") or channel
                    except Exception:
                        pass

                posts.append({
                    "url": f"https://www.youtube.com/watch?v={vid_id}",
                    "kol_name": channel, "platform": "YouTube",
                    "description": title,
                    "views": views,
                    "likes": likes,
                    "comments": comments,
                    "saves": None,
                    "hashtags": re.findall(r"#[\w\u3000-\u9fff]+", title),
                })
            except Exception:
                continue

        if posts:
            result["success"] = True
            result["posts"] = posts
        else:
            result["error"] = "YouTube 검색 결과 없음 (yt-dlp 필요)"
        return result

    except Exception as e:
        result["error"] = f"yt-dlp 오류: {e}"
        return result


def search_brand(brand: str, platforms: list[str],
                 max_results: int = 30,
                 playwright_page=None) -> list[dict]:
    """여러 플랫폼에서 브랜드 검색 → 통합 결과 반환"""
    all_posts = []
    errors = []
    for plat in platforms:
        try:
            if plat == "TikTok":
                r = search_tiktok_brand(brand, max_results, playwright_page)
            elif plat == "Instagram":
                r = search_instagram_brand(brand, max_results, playwright_page)
            elif plat == "YouTube":
                r = search_youtube_brand(brand, max_results)
            else:
                continue
            if r["success"]:
                for p in r["posts"]:
                    p["brand"] = brand
                all_posts.extend(r["posts"])
            elif r.get("error"):
                errors.append(f"{plat}: {r['error']}")
        except Exception as e:
            errors.append(f"{plat}: 예외 - {e}")
    # 에러 정보를 리스트 속성으로 첨부
    all_posts_obj = all_posts
    # 별도 함수로 에러 접근 가능하도록 전역에 저장
    search_brand._last_errors = errors
    return all_posts


def extract_kol_candidates(search_posts: list[dict]) -> list[dict]:
    """
    검색 결과 포스트 목록 → 고유 KOL 후보 프로필 URL 목록 추출.
    동일 KOL이 여러 번 등장하면 조회수 합산으로 정렬.
    """
    kol_map: dict[str, dict] = {}  # key: (platform, kol_name)

    for post in search_posts:
        name = (post.get("kol_name") or "").strip()
        plat = post.get("platform", "")
        if not name:
            continue

        key = f"{plat}:{name}"
        if key not in kol_map:
            # 프로필 URL 생성
            if plat == "TikTok":
                profile_url = f"https://www.tiktok.com/@{name}"
            elif plat == "Instagram":
                profile_url = f"https://www.instagram.com/{name}/"
            elif plat == "YouTube":
                profile_url = f"https://www.youtube.com/@{name}"
            else:
                profile_url = ""

            kol_map[key] = {
                "kol_name": name,
                "platform": plat,
                "profile_url": profile_url,
                "post_count": 0,
                "total_views": 0,
            }

        kol_map[key]["post_count"] += 1
        kol_map[key]["total_views"] += int(post.get("views") or 0)

    # 조회수 합산 기준 정렬
    candidates = sorted(kol_map.values(), key=lambda x: x["total_views"], reverse=True)
    return candidates
