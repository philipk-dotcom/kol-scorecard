# KOL 스코어카드 툴 — Claude Code 작업 지시서

## 프로젝트 개요

TikTok·Instagram·YouTube·Twitter/X URL 목록을 업로드하면
**조회수·좋아요·댓글수 등을 자동 크롤링**하고 스코어링 → Excel 출력하는 Streamlit 웹앱.
GitHub → Streamlit Community Cloud에 배포되어 있음.

- 레포: `github.com/philipk-dotcom/kol-scorecard`
- 배포 URL: `https://kol-scorecard.streamlit.app`

---

## 파일 구조

```
kol_tool/
├── app.py            # Streamlit 진입점 (IS_CLOUD 감지, 사이드바 로그인 포함)
├── scraper.py        # 플랫폼별 스크래퍼 (TikTok/Instagram/YouTube/Twitter)
├── scorer.py         # KOL 메트릭 계산 및 스코어링
├── export.py         # Excel 출력
├── db.py             # SQLite 기반 KOL 히스토리 DB
├── pages/
│   ├── page_score.py    # 스크래핑 + 스코어링 페이지
│   ├── page_brand.py    # 브랜드 탐색 페이지
│   ├── page_history.py  # KOL 히스토리 DB 페이지
│   └── page_campaign.py # 캠페인 관리 페이지
├── requirements.txt
├── packages.txt      # apt 패키지 (chromium 포함)
└── .streamlit/
    └── config.toml
```

---

## 크롤링 핵심 아키텍처

### 1. 로컬 vs 클라우드 감지

```python
IS_CLOUD = (
    os.environ.get("STREAMLIT_SHARING_MODE") == "true"
    or os.environ.get("IS_STREAMLIT_CLOUD") == "true"
    or "/mount/src" in os.getcwd()
)
```

- **로컬**: Playwright 브라우저 자동화 가능 → 로그인 상태 유지 크롤링
- **클라우드**: Playwright 없음 → requests + HTML 파싱 fallback 전용

### 2. TikTok 크롤링 방식 (성공 확인)

**2단계 fallback 구조:**

```
Playwright(로그인 세션) → 실패 시 → requests(모바일 UA) → HTML 내 JSON 파싱
```

**requests 방식 — 핵심 포인트:**

```python
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
resp = sess.get(f"https://www.tiktok.com/@{username}", timeout=20)
```

**HTML 내장 JSON 파싱 — 3가지 포맷 순서대로 시도:**

```python
# ① 최신 포맷
m = re.search(r'id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>([\s\S]*?)</script>', resp.text)
# d["__DEFAULT_SCOPE__"]["webapp.video-list"]["itemList"]

# ② 중간 포맷
m = re.search(r'id="SIGI_STATE"[^>]*>([\s\S]*?)</script>', resp.text)
# d["ItemModule"] → dict.values()

# ③ 구버전
m = re.search(r'id="__NEXT_DATA__"[^>]*>([\s\S]*?)</script>', resp.text)
# d["props"]["pageProps"]["itemList"]
```

**지표 추출:**

```python
def _extract_stats(item):
    s = item.get("stats") or item.get("statsV2") or {}
    return {
        "views":    s.get("playCount") or s.get("vvCount"),
        "likes":    s.get("diggCount"),
        "comments": s.get("commentCount"),
        "saves":    s.get("collectCount"),
        "shares":   s.get("shareCount"),
    }
```

### 3. Instagram 크롤링

- **로컬 Playwright 필수** (requests만으로는 로그인 벽 때문에 실패)
- 클라우드에서는 버튼 비활성화, 사용자에게 "로컬 전용" 안내
- 포스트 페이지 HTML에서 `"like_count":(\d+)` 정규식으로 추출

### 4. YouTube 크롤링

- `yt-dlp` 라이브러리 사용 (로그인 불필요, 클라우드 OK)
- `yt_dlp.YoutubeDL({'quiet': True}).extract_info(url, download=False)` → 조회수/좋아요 추출

### 5. Twitter/X 크롤링

- requests + BeautifulSoup 기본 시도
- 클라우드에서 링크 버튼으로 로그인 유도 후 Playwright 재시도

---

## 로그인 버튼 구현 (사이드바)

```python
PLATFORM_LOGIN_INFO = {
    "TikTok":    {"url": "https://www.tiktok.com/login",                             "icon": "🎵"},
    "Instagram": {"url": "https://www.instagram.com/accounts/login/",                "icon": "📸"},
    "YouTube":   {"url": "https://accounts.google.com/ServiceLogin?service=youtube", "icon": "▶️"},
    "Twitter":   {"url": "https://twitter.com/i/flow/login",                         "icon": "🐦"},
}
REQUIRES_LOCAL = {"TikTok", "Instagram"}

for plat, info in PLATFORM_LOGIN_INFO.items():
    col_btn, col_status = st.columns([3, 2])
    with col_btn:
        if IS_CLOUD and plat in REQUIRES_LOCAL:
            # 클라우드: 비활성 버튼
            st.button(f"{info['icon']} {plat} (로컬 전용)", disabled=True, ...)
        elif IS_CLOUD:
            # 클라우드: 링크 버튼 (새 탭 오픈)
            st.link_button(f"{info['icon']} {plat}", url=info["url"], ...)
        else:
            # 로컬: Playwright 세션 오픈
            if st.button(f"{info['icon']} {plat}", ...):
                st.session_state[f"open_browser_{plat.lower()}"] = True
```

---

## 자주 발생하는 에러와 해결책

### 에러 1: `TypeError: unsupported format string passed to NoneType.__format__`

```
st.markdown(f'조회수 {raw["avg_views"]:,}')  # avg_views가 None이면 터짐
```

**해결:**
```python
views_disp = f'{raw["avg_views"]:,}' if raw["avg_views"] is not None else "N/A"
st.markdown(f'조회수 {views_disp}')
```

모든 숫자 포맷팅 시 None 체크를 먼저 할 것.

### 에러 2: `{**some_dict, ...}` 문법 — GitHub 업로드 시 `**` 가 `*@`로 깨짐

원인: 일부 에디터/업로드 툴의 인코딩 문제.
**해결:** 파일 업로드 후 반드시 `python -c "import ast; ast.parse(open('scraper.py').read())"` 로 문법 확인.

### 에러 3: TikTok 스크래핑 "데이터 추출 실패"

원인: TikTok이 봇 감지 → 모바일 UA가 없거나 Referer 헤더 누락.
**해결:** requests 헤더에 반드시 `Referer: https://www.tiktok.com/` 포함.

### 에러 4: Streamlit Cloud 배포 후 `ModuleNotFoundError`

원인: `requirements.txt`에 패키지 누락.
현재 필수 패키지:
```
streamlit>=1.32.0
pandas>=2.0.0
openpyxl>=3.1.0
requests>=2.31.0
beautifulsoup4>=4.12.0
yt-dlp>=2024.1.1
numpy>=1.26.0
pytrends>=4.9.0
```

### 에러 5: `IS_CLOUD` 감지 실패로 Playwright 임포트 시도 → 크래시

```python
# 항상 조건부로 임포트
if not IS_CLOUD:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        pass
```

---

## Streamlit Cloud 배포 필수 파일

**`.streamlit/config.toml`:**
```toml
[server]
headless = true
enableCORS = false

[theme]
primaryColor = "#2e75b6"
```

**`packages.txt`** (Playwright용, 로컬 실행 시):
```
chromium-browser
```

**GitHub Actions 없이 배포:** GitHub push만 하면 Streamlit Cloud 자동 재배포.
Main file path: `app.py`

---

## 작업 요청 시 주의사항

1. **`scraper.py` 수정 후 반드시 문법 확인**: `python -m py_compile scraper.py`
2. **숫자 포맷팅**: 항상 `None` 체크 후 `:,` 적용
3. **플랫폼 감지 로직**: `detect_platform(url)` 함수가 URL에서 자동 감지
4. **핀 게시물 제외**: `pinned_ids` 파라미터로 핀 포스트 ID 집합 전달
5. **avg_* 필드**: 스크래핑 실패 시 `None`, 성공 시 `int` (반올림)
6. **`success: False`인데 avg_views 등을 포맷팅하지 말 것**

---

## 현재 구현된 기능

- [x] TikTok/Instagram/YouTube/Twitter 스크래핑
- [x] KOL 스코어링 (팔로워·조회수·인게이지먼트 기반)
- [x] Excel 스코어카드 출력
- [x] SQLite KOL 히스토리 DB
- [x] 캠페인 관리 (유료 포스팅 등록·ROI 추적)
- [x] Streamlit Community Cloud 배포

## 구현 예정 기능 (작업 지시서 참조)

- [ ] 브랜드 콘텐츠 실시간 탐색 (최근 3개월, 브랜드명 입력)
- [ ] KOL 자동 발굴 → 자동 스코어링 → 컨택 리스트 생성
- [ ] 유료 포스팅 실적 집계 및 CPV·CPE 분석
- [ ] 노출수-검색량 상관관계 분석 (Google Trends + Qoo10 + Amazon JP)
- [ ] KOL 오디언스 품질 분석 (가짜 팔로워 감지)
- [ ] 캠페인 예산 시뮬레이터
- [ ] 포스팅 소재 유형별 CPV 분석
