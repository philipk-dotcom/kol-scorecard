"""
KOL 자동 스코어카드 생성기 v2.0
Streamlit 웹 앱 — 멀티 페이지 구조

실행: streamlit run app.py
"""

import os
import streamlit as st
import pandas as pd
import numpy as np
import json
import time
import re
import io
from pathlib import Path
from copy import deepcopy

# Streamlit Community Cloud 환경 감지
IS_CLOUD = (
    os.environ.get("STREAMLIT_SHARING_MODE") == "true"
    or os.environ.get("IS_STREAMLIT_CLOUD") == "true"
    or "/mount/src" in os.getcwd()
)

from scraper import (scrape_kol, detect_platform, extract_username,
                     search_brand, extract_kol_candidates)
from scorer import (KOLMetrics, calculate_all_scores, kols_to_dataframe,
                    analyze_audience_quality, PLATFORM_WEIGHTS)
from export import generate_excel_scorecard
from db import (
    init_db, save_scored_df, get_all_kols, get_kol_history, get_kol_delta,
    update_campaign_status, update_memo, delete_kol, get_db_stats,
    create_campaign, get_campaigns, add_paid_post, update_paid_post_metrics,
    get_paid_posts, get_campaign_summary, delete_campaign, get_daily_impressions,
)

# DB 초기화
init_db()

# ──────────────────────────────────────────────────────────────
#  페이지 설정
# ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="KOL 스코어카드 v2.0",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ──────────────────────────────────────────────────────────────
#  공통 CSS — 전환 효과, 호버, 메뉴 스타일
# ──────────────────────────────────────────────────────────────
st.markdown("""
<style>
/* 전체 배경 */
.stApp { background: #f4f6fa; }

/* 부드러운 전환 */
* { transition: all 0.2s ease-in-out; }

/* 버튼 호버 효과 */
.stButton > button {
    transition: all 0.25s ease;
    border-radius: 8px;
}
.stButton > button:hover {
    transform: translateY(-1px);
    box-shadow: 0 4px 12px rgba(46,117,182,0.3);
    filter: brightness(1.08);
}

/* 메뉴 버튼 */
div[data-testid="stHorizontalBlock"] .stButton > button {
    border-radius: 20px;
    font-weight: 600;
}

/* 헤더 배너 */
.kol-header {
    background: linear-gradient(135deg, #1a3a5c 0%, #2e75b6 60%, #1f7a8c 100%);
    padding: 1.4rem 2rem;
    border-radius: 12px;
    margin-bottom: 1.2rem;
    color: white;
    animation: fadeIn 0.5s ease;
}
.kol-header h1 { font-size: 1.6rem; font-weight: 800; margin: 0; }
.kol-header p  { font-size: 0.88rem; opacity: 0.85; margin: 0.2rem 0 0; }

/* 페이드인 애니메이션 */
@keyframes fadeIn {
    from { opacity: 0; transform: translateY(10px); }
    to   { opacity: 1; transform: translateY(0); }
}

/* 섹션 카드 */
.section-card {
    background: white;
    border-radius: 10px;
    padding: 1.2rem 1.5rem;
    margin-bottom: 1rem;
    box-shadow: 0 2px 8px rgba(0,0,0,0.07);
    border-left: 4px solid #2e75b6;
    animation: fadeIn 0.4s ease;
}

/* 스텝 배지 */
.step-badge {
    display: inline-block;
    background: #2e75b6;
    color: white;
    border-radius: 20px;
    padding: 2px 12px;
    font-size: 0.8rem;
    font-weight: 700;
    margin-right: 6px;
}

/* 사이드바 정보 */
.sidebar-info {
    background: #e8f4fd;
    border-radius: 8px;
    padding: 0.8rem;
    font-size: 0.85rem;
    margin-bottom: 0.8rem;
}

/* 메트릭 카드 */
div[data-testid="stMetric"] {
    background: white;
    border-radius: 10px;
    padding: 12px;
    box-shadow: 0 2px 6px rgba(0,0,0,0.06);
}

/* 링크 버튼 스타일 */
.link-btn {
    display: inline-block;
    background: #e8f4fd;
    color: #1a3a5c;
    padding: 3px 10px;
    border-radius: 6px;
    text-decoration: none;
    font-size: 0.8rem;
    transition: all 0.2s;
}
.link-btn:hover {
    background: #2e75b6;
    color: white;
}

/* 등급 색상 */
.grade-5 { color: #1a7a1a; font-weight: 800; }
.grade-4 { color: #1a4a8a; font-weight: 700; }
.grade-3 { color: #8a6a00; font-weight: 600; }

/* 스크래핑 상태 */
.scrape-ok   { color: #1a7a1a; }
.scrape-fail { color: #cc4400; }

/* 탭 전환 애니메이션 */
.stTabs [data-baseweb="tab-panel"] {
    animation: fadeIn 0.3s ease;
}
</style>
""", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────
#  사이드바 — 플랫폼별 로그인
# ──────────────────────────────────────────────────────────────
PLATFORM_LOGIN_INFO = {
    "TikTok":    {"url": "https://www.tiktok.com/login",                        "icon": "🎵"},
    "Instagram": {"url": "https://www.instagram.com/accounts/login/",           "icon": "📸"},
    "YouTube":   {"url": "https://accounts.google.com/ServiceLogin?service=youtube", "icon": "▶️"},
    "Twitter":   {"url": "https://twitter.com/i/flow/login",                    "icon": "🐦"},
}

_LOGIN_URLS = {k.lower(): v["url"] for k, v in PLATFORM_LOGIN_INFO.items()}

with st.sidebar:
    st.markdown("### ⚙️ 설정")

    num_posts = st.number_input(
        "분석 게시물 수", min_value=3, max_value=30, value=12, step=1,
        help="핀 게시물 제외 후 최신 N개 게시물 평균 사용"
    )

    st.markdown("---")
    st.markdown("### 🔑 플랫폼별 로그인")

    if IS_CLOUD:
        st.markdown(
            '<div class="sidebar-info">'
            '☁️ 클라우드 버전에서는 Playwright 자동 수집이 제한됩니다.<br>'
            '수집 실패 시 직접 수치를 입력해 점수를 계산하세요.'
            '</div>',
            unsafe_allow_html=True
        )
    else:
        st.markdown(
            '<div class="sidebar-info">'
            '로컬에서는 버튼 클릭 시 Playwright 브라우저가 열립니다.<br>'
            '로그인 후 브라우저를 닫으면 세션이 저장됩니다.'
            '</div>',
            unsafe_allow_html=True
        )

    # 플랫폼별 로그인 버튼 (클라우드·로컬 모두 표시)
    for plat, info in PLATFORM_LOGIN_INFO.items():
        login_key = f"login_{plat.lower()}"
        if login_key not in st.session_state:
            st.session_state[login_key] = False

        col_btn, col_status = st.columns([3, 2])
        with col_btn:
            if IS_CLOUD:
                # 클라우드: 새 탭에서 로그인 URL 열기
                st.markdown(
                    f'<a href="{info["url"]}" target="_blank" '
                    f'style="display:block;text-align:center;background:#2e75b6;color:white;'
                    f'padding:6px 12px;border-radius:8px;text-decoration:none;font-size:0.85rem;'
                    f'font-weight:600;transition:all 0.2s;"'
                    f'onmouseover="this.style.background=\'#1a5a9c\'"'
                    f'onmouseout="this.style.background=\'#2e75b6\'">'
                    f'{info["icon"]} {plat} 로그인</a>',
                    unsafe_allow_html=True
                )
            else:
                if st.button(f"{info['icon']} {plat} 로그인", key=f"btn_{login_key}", use_container_width=True):
                    st.session_state[f"open_browser_{plat.lower()}"] = True
        with col_status:
            if st.session_state[login_key]:
                st.markdown("✅ 완료")
            else:
                st.markdown("🔒 필요")

    st.markdown("---")
    st.markdown("### 📌 핀 게시물 ID")
    pinned_raw = st.text_area(
        "핀 게시물 ID (줄바꿈/쉼표 구분)", height=60,
        placeholder="7301234567890123456", label_visibility="collapsed"
    )
    pinned_global = [p.strip() for p in re.split(r"[,\n]", pinned_raw) if p.strip()]

    st.markdown("---")
    st.caption("KOL 선별 프레임워크 v2.0")

# ──────────────────────────────────────────────────────────────
#  플랫폼별 브라우저 로그인 처리
# ──────────────────────────────────────────────────────────────
for _plat_key, _login_url in _LOGIN_URLS.items():
    if st.session_state.get(f"open_browser_{_plat_key}"):
        st.session_state[f"open_browser_{_plat_key}"] = False
        try:
            from playwright.sync_api import sync_playwright
            browser_dir = Path.home() / ".kol_tool_session"
            browser_dir.mkdir(exist_ok=True)
            with sync_playwright() as p:
                ctx = p.chromium.launch_persistent_context(
                    str(browser_dir), headless=False, args=["--start-maximized"]
                )
                page = ctx.new_page()
                page.goto(_login_url)
                st.info(f"🌐 {_plat_key.title()} 로그인 페이지가 열렸습니다. 로그인 후 브라우저를 닫아주세요.")
                ctx.wait_for_event("close", timeout=300000)
                ctx.close()
                st.session_state[f"login_{_plat_key}"] = True
                st.success(f"✅ {_plat_key.title()} 로그인 완료!")
        except Exception as e:
            st.error(f"{_plat_key.title()} 브라우저 오류: {e}")

# ──────────────────────────────────────────────────────────────
#  헤더 + 메뉴 네비게이션
# ──────────────────────────────────────────────────────────────
st.markdown("""
<div class="kol-header">
  <h1>🎯 KOL 스코어카드 v2.0</h1>
  <p>URL + 비용 입력 → 자동 수집 → 점수화 → 콘텐츠 탐색 → 인사이트</p>
</div>
""", unsafe_allow_html=True)

# 메뉴 버튼
if "current_page" not in st.session_state:
    st.session_state.current_page = "score"

menu_cols = st.columns(3)
with menu_cols[0]:
    if st.button("🏆 KOL 스코어 시스템", use_container_width=True,
                 type="primary" if st.session_state.current_page == "score" else "secondary"):
        st.session_state.current_page = "score"
        st.rerun()
with menu_cols[1]:
    if st.button("🔎 콘텐츠 실시간 탐색", use_container_width=True,
                 type="primary" if st.session_state.current_page == "explore" else "secondary"):
        st.session_state.current_page = "explore"
        st.rerun()
with menu_cols[2]:
    if st.button("📊 분석 & 관리 도구", use_container_width=True,
                 type="primary" if st.session_state.current_page == "tools" else "secondary"):
        st.session_state.current_page = "tools"
        st.rerun()

st.markdown("---")

# ══════════════════════════════════════════════════════════════
#  Menu 1: KOL 스코어 시스템
# ══════════════════════════════════════════════════════════════
if st.session_state.current_page == "score":
    from pages.page_score import render_score_page
    render_score_page(
        IS_CLOUD=IS_CLOUD,
        num_posts=num_posts,
        pinned_global=pinned_global,
    )

# ══════════════════════════════════════════════════════════════
#  Menu 2: 콘텐츠 실시간 탐색
# ══════════════════════════════════════════════════════════════
elif st.session_state.current_page == "explore":
    from pages.page_explore import render_explore_page
    render_explore_page(IS_CLOUD=IS_CLOUD)

# ══════════════════════════════════════════════════════════════
#  Menu 3: 분석 & 관리 도구
# ══════════════════════════════════════════════════════════════
elif st.session_state.current_page == "tools":
    from pages.page_tools import render_tools_page
    render_tools_page(IS_CLOUD=IS_CLOUD, num_posts=num_posts)

# ──────────────────────────────────────────────────────────────
#  푸터
# ──────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    '<div style="text-align:center; color:#888; font-size:0.8rem;">'
    'KOL 선별 프레임워크 v2.0 | CPV × Engagement Efficiency | '
    '벤치마크: Rival IQ 2024, Dash Social 2025, JapanBuzz 2025'
    '</div>',
    unsafe_allow_html=True
)
