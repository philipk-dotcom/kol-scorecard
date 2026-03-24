"""
KOL 자동 스코어카드 생성기
Streamlit 웹 앱 (로컬 실행)

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
from scorer import KOLMetrics, calculate_all_scores, kols_to_dataframe, analyze_audience_quality
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
#  설정 & 스타일
# ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="KOL 스코어카드 자동 생성기",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded",
)

CUSTOM_CSS = """
<style>
/* 전체 배경 */
.stApp { background: #f4f6fa; }

/* 헤더 배너 */
.kol-header {
    background: linear-gradient(135deg, #1a3a5c 0%, #2e75b6 60%, #1f7a8c 100%);
    padding: 1.6rem 2rem;
    border-radius: 12px;
    margin-bottom: 1.5rem;
    color: white;
}
.kol-header h1 { font-size: 1.8rem; font-weight: 800; margin: 0; }
.kol-header p  { font-size: 0.92rem; opacity: 0.85; margin: 0.3rem 0 0; }

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

/* 섹션 카드 */
.section-card {
    background: white;
    border-radius: 10px;
    padding: 1.2rem 1.5rem;
    margin-bottom: 1rem;
    box-shadow: 0 2px 8px rgba(0,0,0,0.07);
    border-left: 4px solid #2e75b6;
}

/* 플랫폼 뱃지 */
.badge-tiktok    { background:#e8f4fd; color:#1a3a5c; padding:2px 8px; border-radius:12px; font-size:0.8rem; }
.badge-instagram { background:#fde8f4; color:#7b0038; padding:2px 8px; border-radius:12px; font-size:0.8rem; }
.badge-youtube   { background:#fdf0e8; color:#8b2500; padding:2px 8px; border-radius:12px; font-size:0.8rem; }
.badge-twitter   { background:#e8effe; color:#00218a; padding:2px 8px; border-radius:12px; font-size:0.8rem; }
.badge-lipscosme { background:#f4e8fd; color:#4a007b; padding:2px 8px; border-radius:12px; font-size:0.8rem; }

/* 등급 색상 */
.grade-5 { color: #1a7a1a; font-weight: 800; }
.grade-4 { color: #1a4a8a; font-weight: 700; }
.grade-3 { color: #8a6a00; font-weight: 600; }

/* 결과 테이블 행 */
.metric-ok   { color: #1a7a1a; }
.metric-warn { color: #8a6a00; }
.metric-err  { color: #cc0000; }

/* 성공/실패 표시 */
.scrape-ok   { color: #1a7a1a; }
.scrape-fail { color: #cc4400; }

/* 사이드바 */
.sidebar-info {
    background: #e8f4fd;
    border-radius: 8px;
    padding: 0.8rem;
    font-size: 0.85rem;
    margin-bottom: 0.8rem;
}
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────
#  사이드바
# ──────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ⚙️ 설정")

    num_posts = st.number_input(
        "분석 게시물 수",
        min_value=3, max_value=30, value=12, step=1,
        help="핀 게시물 제외 후 최신 N개 게시물 평균 사용"
    )

    st.markdown("---")
    st.markdown("### 🔑 플랫폼별 로그인")

    PLATFORM_LOGIN_INFO = {
        "TikTok": {"url": "https://www.tiktok.com/login", "icon": "🎵"},
        "Instagram": {"url": "https://www.instagram.com/accounts/login/", "icon": "📸"},
        "YouTube": {"url": "https://accounts.google.com/ServiceLogin?service=youtube", "icon": "▶️"},
        "Twitter": {"url": "https://twitter.com/i/flow/login", "icon": "🐦"},
    }

    if IS_CLOUD:
        st.markdown(
            '<div class="sidebar-info">'
            '☁️ 클라우드 버전에서는 자동 수집이 제한될 수 있습니다.<br>'
            '수집 실패 시 STEP 3에서 직접 수치를 입력해 점수를 계산하세요.'
            '</div>',
            unsafe_allow_html=True
        )
    else:
        st.markdown(
            '<div class="sidebar-info">'
            '플랫폼별로 로그인하세요. 한 번 로그인하면 세션이 유지됩니다.'
            '</div>',
            unsafe_allow_html=True
        )
        for plat, info in PLATFORM_LOGIN_INFO.items():
            login_key = f"login_{plat.lower()}"
            # 세션 상태 초기화
            if login_key not in st.session_state:
                st.session_state[login_key] = False

            col_btn, col_status = st.columns([3, 2])
            with col_btn:
                if st.button(
                    f"{info['icon']} {plat} 로그인",
                    key=f"btn_{login_key}",
                    use_container_width=True,
                ):
                    st.session_state[f"open_browser_{plat.lower()}"] = True
            with col_status:
                if st.session_state[login_key]:
                    st.markdown("✅ 로그인됨")
                else:
                    st.markdown("🔒 필요")

    st.markdown("---")
    st.markdown("### 📌 핀 게시물 ID 관리")
    st.caption("TikTok·Lipscosme에서 핀으로 고정된 게시물 ID를 제외할 수 있습니다.")
    pinned_raw = st.text_area(
        "핀 게시물 ID (줄바꿈 또는 쉼표로 구분)",
        height=80,
        placeholder="7301234567890123456\n7298765432109876543"
    )
    pinned_global = [
        p.strip() for p in re.split(r"[,\n]", pinned_raw) if p.strip()
    ]

    st.markdown("---")
    st.caption("KOL 선별 프레임워크 v1.0 | CPV × Engagement")


# ──────────────────────────────────────────────────────────────
#  헤더
# ──────────────────────────────────────────────────────────────
st.markdown("""
<div class="kol-header">
  <h1>🎯 KOL 자동 스코어카드 생성기</h1>
  <p>URL + 캐스팅 비용 입력 → 지표 자동 수집 → CPV × Engagement 점수화 → Excel 다운로드</p>
</div>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────────────────────
#  세션 상태 초기화
# ──────────────────────────────────────────────────────────────
if "kol_rows" not in st.session_state:
    st.session_state.kol_rows = pd.DataFrame({
        "KOL명":       [""],
        "URL":         [""],
        "플랫폼":      ["자동감지"],
        "비용(JPY)":   [0],
        "핀게시물ID":  [""],
    })

if "scraped_results" not in st.session_state:
    st.session_state.scraped_results = None

if "scored_df" not in st.session_state:
    st.session_state.scored_df = None

# ──────────────────────────────────────────────────────────────
#  플랫폼별 브라우저 로그인 처리
# ──────────────────────────────────────────────────────────────
_LOGIN_URLS = {
    "tiktok":    "https://www.tiktok.com/login",
    "instagram": "https://www.instagram.com/accounts/login/",
    "youtube":   "https://accounts.google.com/ServiceLogin?service=youtube",
    "twitter":   "https://twitter.com/i/flow/login",
}

for _plat_key, _login_url in _LOGIN_URLS.items():
    if st.session_state.get(f"open_browser_{_plat_key}"):
        st.session_state[f"open_browser_{_plat_key}"] = False
        try:
            from playwright.sync_api import sync_playwright
            browser_dir = Path.home() / ".kol_tool_session"
            browser_dir.mkdir(exist_ok=True)
            with sync_playwright() as p:
                ctx = p.chromium.launch_persistent_context(
                    str(browser_dir),
                    headless=False,
                    args=["--start-maximized"]
                )
                page = ctx.new_page()
                page.goto(_login_url)
                st.info(f"🌐 {_plat_key.title()} 로그인 페이지가 열렸습니다. 로그인 후 브라우저를 닫아주세요.")
                ctx.wait_for_event("close", timeout=300000)
                ctx.close()
                st.session_state[f"login_{_plat_key}"] = True
                st.success(f"✅ {_plat_key.title()} 로그인 완료!")
        except Exception as e:
            st.error(f"{_plat_key.title()} 브라우저를 열 수 없습니다: {e}")


# ──────────────────────────────────────────────────────────────
#  STEP 1: KOL 정보 입력
# ──────────────────────────────────────────────────────────────
st.markdown('<span class="step-badge">STEP 1</span> **KOL 정보 입력**', unsafe_allow_html=True)

tab_manual, tab_csv = st.tabs(["✏️ 직접 입력", "📂 CSV/Excel 업로드"])

with tab_manual:
    st.caption("URL을 입력하면 플랫폼이 자동 감지됩니다. 행 추가는 아래 버튼을 클릭하세요.")

    # 행 추가 / 초기화
    col_add, col_clear, col_sample = st.columns([1, 1, 2])
    with col_add:
        if st.button("➕ 행 추가", use_container_width=True):
            new_row = pd.DataFrame({
                "KOL명": [""], "URL": [""], "플랫폼": ["자동감지"],
                "비용(JPY)": [0], "핀게시물ID": [""]
            })
            st.session_state.kol_rows = pd.concat(
                [st.session_state.kol_rows, new_row], ignore_index=True
            )
    with col_clear:
        if st.button("🗑 초기화", use_container_width=True):
            st.session_state.kol_rows = pd.DataFrame({
                "KOL명": [""], "URL": [""], "플랫폼": ["자동감지"],
                "비용(JPY)": [0], "핀게시물ID": [""]
            })
            st.session_state.scraped_results = None
            st.session_state.scored_df = None
    with col_sample:
        if st.button("📋 샘플 데이터 로드", use_container_width=True):
            st.session_state.kol_rows = pd.DataFrame({
                "KOL명": [
                    "가연がよん", "もみー", "츠지짱",
                    "𝙧𝙪𝙣.", "苺鈴"
                ],
                "URL": [
                    "https://www.tiktok.com/@gayon_official",
                    "https://www.instagram.com/momii_beauty",
                    "https://www.youtube.com/@tsujichan_beauty",
                    "https://twitter.com/run_beauty_jp",
                    "https://lipscosme.com/users/@ichigo_makeup",
                ],
                "플랫폼": ["TikTok", "Instagram", "YouTube", "Twitter", "Lipscosme"],
                "비용(JPY)": [150000, 80000, 200000, 60000, 50000],
                "핀게시물ID": ["", "", "", "", ""],
            })

    # 데이터 에디터
    edited = st.data_editor(
        st.session_state.kol_rows,
        use_container_width=True,
        num_rows="dynamic",
        column_config={
            "KOL명": st.column_config.TextColumn("KOL명", width="medium"),
            "URL": st.column_config.TextColumn(
                "URL (필수)",
                width="large",
                help="tiktok.com / instagram.com / youtube.com / twitter.com / x.com / lipscosme.com"
            ),
            "플랫폼": st.column_config.SelectboxColumn(
                "플랫폼",
                options=["자동감지", "TikTok", "Instagram", "YouTube", "Twitter", "Lipscosme"],
                width="medium",
            ),
            "비용(JPY)": st.column_config.NumberColumn(
                "캐스팅 비용(JPY)", min_value=0, step=1000, format="¥%d", width="medium"
            ),
            "핀게시물ID": st.column_config.TextColumn(
                "핀 게시물 ID (선택)",
                width="medium",
                help="쉼표로 구분. 해당 행 KOL의 핀 게시물 ID만 입력"
            ),
        },
        hide_index=True,
        key="kol_editor",
    )
    st.session_state.kol_rows = edited

    # URL 입력 시 플랫폼 자동 감지 미리보기
    if not edited.empty:
        preview_rows = []
        for _, row in edited.iterrows():
            url = str(row.get("URL", "")).strip()
            if not url:
                continue
            plat = (str(row.get("플랫폼", "자동감지")).strip()
                    if str(row.get("플랫폼", "")).strip() != "자동감지"
                    else detect_platform(url))
            uname = extract_username(url, plat) if url else ""
            preview_rows.append({
                "KOL명": row.get("KOL명", ""),
                "감지된 플랫폼": plat,
                "유저명": uname,
                "비용": f"¥{int(row.get('비용(JPY)') or 0):,}" if pd.notna(row.get('비용(JPY)')) else "¥0",
            })

        if preview_rows:
            st.markdown("**📍 입력 미리보기**")
            st.dataframe(
                pd.DataFrame(preview_rows),
                use_container_width=True,
                hide_index=True
            )

with tab_csv:
    st.caption(
        "CSV 또는 Excel 파일을 업로드하세요. "
        "필수 열: **URL**, 선택 열: KOL명 / 비용(JPY) / 핀게시물ID"
    )
    uploaded = st.file_uploader(
        "파일 선택", type=["csv", "xlsx", "xls"],
        label_visibility="collapsed"
    )
    if uploaded:
        try:
            if uploaded.name.endswith(".csv"):
                df_up = pd.read_csv(uploaded)
            else:
                df_up = pd.read_excel(uploaded)

            # 열 정규화
            col_map = {}
            for c in df_up.columns:
                cl = str(c).strip().lower()
                if "url" in cl:
                    col_map[c] = "URL"
                elif "kol" in cl or "name" in cl or "이름" in cl or "명" in cl:
                    col_map[c] = "KOL명"
                elif "비용" in cl or "fee" in cl or "cost" in cl or "단가" in cl:
                    col_map[c] = "비용(JPY)"
                elif "핀" in cl or "pin" in cl:
                    col_map[c] = "핀게시물ID"
            df_up = df_up.rename(columns=col_map)

            for req in ["URL"]:
                if req not in df_up.columns:
                    st.error(f"'{req}' 열이 없습니다.")
                    st.stop()

            if "KOL명"      not in df_up.columns: df_up["KOL명"]      = ""
            if "비용(JPY)"  not in df_up.columns: df_up["비용(JPY)"]  = 0
            if "핀게시물ID" not in df_up.columns: df_up["핀게시물ID"] = ""
            df_up["플랫폼"] = "자동감지"

            df_up = df_up[["KOL명", "URL", "플랫폼", "비용(JPY)", "핀게시물ID"]]
            st.session_state.kol_rows = df_up
            st.success(f"✅ {len(df_up)}명 로드 완료")
            st.dataframe(df_up, use_container_width=True, hide_index=True)

        except Exception as e:
            st.error(f"파일 읽기 오류: {e}")

    # CSV 템플릿 다운로드
    template_df = pd.DataFrame({
        "KOL명":       ["가연がよん", "もみー"],
        "URL":         ["https://www.tiktok.com/@...", "https://www.instagram.com/..."],
        "비용(JPY)":   [150000, 80000],
        "핀게시물ID":  ["7301234567890,7298765432109", ""],
    })
    csv_bytes = template_df.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        "📥 CSV 템플릿 다운로드",
        data=csv_bytes,
        file_name="kol_input_template.csv",
        mime="text/csv"
    )


# ──────────────────────────────────────────────────────────────
#  STEP 2: 스크래핑 실행
# ──────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown('<span class="step-badge">STEP 2</span> **자동 스크래핑**', unsafe_allow_html=True)

df_input = st.session_state.kol_rows.copy()
valid_rows = df_input[df_input["URL"].str.strip().str.len() > 5].copy()

if valid_rows.empty:
    st.info("STEP 1에서 URL을 먼저 입력해주세요.")
else:
    col_scrape, col_info = st.columns([1, 3])
    with col_scrape:
        run_scrape = st.button(
            "🚀 스크래핑 시작",
            use_container_width=True,
            type="primary",
            help="입력된 모든 URL을 방문해 지표를 자동 수집합니다."
        )
    with col_info:
        st.markdown(
            f"**{len(valid_rows)}명** 스크래핑 예정 | "
            f"예상 소요 시간: **{len(valid_rows) * 15 // 60 + 1}~{len(valid_rows) * 30 // 60 + 2}분**"
        )

    if run_scrape:
        results = []
        progress = st.progress(0, text="스크래핑 준비 중...")
        status_container = st.container()
        browser_dir = Path.home() / ".kol_tool_session"

        # Playwright 세션 가져오기 (없으면 None으로 진행)
        pw_ctx = None
        try:
            from playwright.sync_api import sync_playwright
            _pw = sync_playwright().__enter__()
            if browser_dir.exists():
                pw_ctx = _pw.chromium.launch_persistent_context(
                    str(browser_dir),
                    headless=True,
                )
        except Exception:
            pass

        for i, (_, row) in enumerate(valid_rows.iterrows()):
            url     = str(row.get("URL", "")).strip()

            # 플랫폼: "None"·"nan"·빈값이면 URL에서 자동 감지
            plat_override = str(row.get("플랫폼", "")).strip()
            if plat_override in ("자동감지", "None", "nan", ""):
                plat = detect_platform(url)
            else:
                plat = plat_override

            # KOL명: "None"·빈값이면 URL에서 유저명 추출
            name_raw = str(row.get("KOL명", "")).strip()
            if not name_raw or name_raw in ("None", "nan"):
                name_raw = extract_username(url, plat)
            name = name_raw or url

            fee_raw = row.get("비용(JPY)", 0)
            fee     = float(fee_raw) if fee_raw and str(fee_raw) not in ("", "nan", "None") else None

            pin_raw = str(row.get("핀게시물ID", "")).strip()
            row_pins = [p.strip() for p in re.split(r"[,\n]", pin_raw) if p.strip()]
            all_pins = list(set(pinned_global + row_pins))

            progress.progress(
                (i + 0.5) / len(valid_rows),
                text=f"[{i+1}/{len(valid_rows)}] {name} ({plat}) 스크래핑 중..."
            )

            pw_page = None
            if pw_ctx:
                try:
                    pw_page = pw_ctx.new_page()
                except Exception:
                    pw_page = None

            raw = scrape_kol(
                url=url,
                num_posts=num_posts,
                pinned_ids=all_pins,
                playwright_page=pw_page
            )

            if pw_page:
                try:
                    pw_page.close()
                except Exception:
                    pass

            results.append({
                "name":    name,
                "platform":plat,
                "url":     url,
                "fee":     fee,
                **raw
            })

            # 실시간 결과 표시
            with status_container:
                if raw["success"]:
                    st.markdown(
                        f'<span class="scrape-ok">✅ **{name}** ({plat}) — '
                        f'조회수 {raw["avg_views"]:,}회 / 좋아요 {raw["avg_likes"]:,} '
                        f'| {raw["post_count"]}게시물 분석</span>',
                        unsafe_allow_html=True
                    )
                else:
                    st.markdown(
                        f'<span class="scrape-fail">❌ **{name}** ({plat}) — '
                        f'{raw["error"]}</span>',
                        unsafe_allow_html=True
                    )
            time.sleep(0.5)

        if pw_ctx:
            try:
                pw_ctx.close()
                _pw.__exit__(None, None, None)
            except Exception:
                pass

        progress.progress(1.0, text="✅ 스크래핑 완료!")
        st.session_state.scraped_results = results
        st.session_state.scored_df = None  # 점수 재계산 필요
        st.success(
            f"✅ {sum(1 for r in results if r['success'])} / {len(results)} 성공 | "
            f"{sum(1 for r in results if not r['success'])} 실패"
        )


# ──────────────────────────────────────────────────────────────
#  STEP 3: 수동 보정 & 점수 계산
# ──────────────────────────────────────────────────────────────
if st.session_state.scraped_results:
    st.markdown("---")
    st.markdown('<span class="step-badge">STEP 3</span> **지표 확인 및 수동 보정**', unsafe_allow_html=True)
    st.caption("스크래핑 결과를 확인하고 잘못된 값은 직접 수정하세요. 비어있는 셀은 '—'로 표시됩니다.")

    # 결과를 편집 가능한 DataFrame으로 변환
    metric_rows = []
    for r in st.session_state.scraped_results:
        metric_rows.append({
            "KOL명":      r["name"],
            "플랫폼":     r["platform"],
            "비용(JPY)":  r["fee"],
            "평균 조회수": r.get("avg_views"),
            "평균 좋아요": r.get("avg_likes"),
            "평균 댓글":   r.get("avg_comments"),
            "평균 저장":   r.get("avg_saves"),
            "평균 공유":   r.get("avg_shares"),
            "게시물 수":   r.get("post_count", 0),
            "URL":        r["url"],
            "오류":       r.get("error", ""),
        })

    metric_df = pd.DataFrame(metric_rows)

    edited_metrics = st.data_editor(
        metric_df.drop(columns=["URL", "오류"]),
        use_container_width=True,
        hide_index=True,
        column_config={
            "KOL명":      st.column_config.TextColumn("KOL명", width="medium"),
            "플랫폼":     st.column_config.TextColumn("플랫폼", width="small"),
            "비용(JPY)":  st.column_config.NumberColumn("비용(JPY)", format="¥%d"),
            "평균 조회수": st.column_config.NumberColumn("평균 조회수", format="%d"),
            "평균 좋아요": st.column_config.NumberColumn("평균 좋아요", format="%d"),
            "평균 댓글":   st.column_config.NumberColumn("평균 댓글",   format="%d"),
            "평균 저장":   st.column_config.NumberColumn("평균 저장",   format="%d"),
            "평균 공유":   st.column_config.NumberColumn("평균 공유",   format="%d"),
            "게시물 수":   st.column_config.NumberColumn("게시물 수",   format="%d"),
        },
        key="metric_editor"
    )

    # 오류 있는 항목 표시
    failed = [(r["name"], r.get("error","")) for r in st.session_state.scraped_results
              if not r.get("success")]
    if failed:
        with st.expander(f"⚠️ 스크래핑 실패 항목 ({len(failed)}개)"):
            for name, err in failed:
                st.markdown(f"- **{name}**: {err}")

    # 점수 계산 버튼
    st.markdown("---")
    st.markdown('<span class="step-badge">STEP 4</span> **점수 계산 & 결과**', unsafe_allow_html=True)

    col_calc, col_dl = st.columns([1, 1])
    with col_calc:
        calc_btn = st.button(
            "🏆 점수 계산",
            use_container_width=True,
            type="primary"
        )

    if calc_btn:
        # KOLMetrics 리스트 생성
        kol_list = []
        for idx, row in edited_metrics.iterrows():
            def _safe(val):
                if val is None: return None
                try:
                    f = float(val)
                    return None if np.isnan(f) else f
                except Exception:
                    return None

            kol = KOLMetrics(
                name     = str(row["KOL명"]),
                platform = str(row["플랫폼"]),
                url      = metric_df.iloc[idx]["URL"],
                fee      = _safe(row.get("비용(JPY)")),
                avg_views    = _safe(row.get("평균 조회수")),
                avg_likes    = _safe(row.get("평균 좋아요")),
                avg_comments = _safe(row.get("평균 댓글")),
                avg_saves    = _safe(row.get("평균 저장")),
                avg_shares   = _safe(row.get("평균 공유")),
                post_count   = int(row.get("게시물 수", 0) or 0),
            )
            kol_list.append(kol)

        scored = calculate_all_scores(kol_list)
        result_df = kols_to_dataframe(scored)
        st.session_state.scored_df = result_df

        # 오디언스 품질 분석
        st.session_state.quality_warnings = analyze_audience_quality(scored)

        # DB 자동 저장
        saved_count = save_scored_df(result_df)
        st.success(f"✅ 점수 계산 완료! ({saved_count}명 DB 저장됨)")


# ──────────────────────────────────────────────────────────────
#  STEP 4: 결과 표시 & Excel 다운로드
# ──────────────────────────────────────────────────────────────
if st.session_state.scored_df is not None:
    df_result = st.session_state.scored_df

    # 채택 권고 요약
    st.markdown("### 🏆 선별 결과 요약")
    adopt_df = df_result[df_result["채택권고"].str.contains("채택", na=False)]

    # 플랫폼별 탑 KOL 카드
    platforms_present = df_result["플랫폼"].dropna().unique().tolist()
    ordered = [p for p in ["TikTok","Instagram","YouTube","Twitter","Lipscosme"]
               if p in platforms_present]

    cols = st.columns(min(len(ordered), 5))
    plat_icons = {
        "TikTok": "🎵", "Instagram": "📸",
        "YouTube": "▶️", "Twitter": "🐦", "Lipscosme": "💄"
    }
    for ci, plat in enumerate(ordered):
        with cols[ci % len(cols)]:
            plat_df = (df_result[df_result["플랫폼"] == plat]
                       .dropna(subset=["종합점수"])
                       .sort_values("종합점수", ascending=False))
            if plat_df.empty:
                continue
            top = plat_df.iloc[0]
            score = top.get("종합점수", 0) or 0
            grade = top.get("등급", "—")

            st.markdown(f"""
            <div style="background:white; border-radius:10px; padding:12px;
                        box-shadow:0 2px 8px rgba(0,0,0,0.1); text-align:center;
                        border-top:4px solid #2e75b6; margin-bottom:8px;">
              <div style="font-size:1.4rem;">{plat_icons.get(plat,'📊')}</div>
              <div style="font-size:0.75rem; color:#666; margin:2px 0;">{plat}</div>
              <div style="font-size:1rem; font-weight:700; color:#1a3a5c;">
                {top.get('KOL명','—')}
              </div>
              <div style="font-size:1.5rem; font-weight:800; color:#2e75b6;">
                {score:.1f}점
              </div>
              <div style="font-size:0.9rem;">{grade}</div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("---")

    # 전체 결과 테이블
    st.markdown("### 📊 전체 스코어카드")

    # 등급 필터
    grade_filter = st.multiselect(
        "등급 필터",
        ["★★★★★", "★★★★", "★★★", "★★", "★"],
        default=["★★★★★", "★★★★", "★★★"],
        label_visibility="collapsed"
    )
    display_df = df_result[df_result["등급"].isin(grade_filter)] if grade_filter else df_result

    # 오디언스 품질 경고 컬럼 추가
    quality_warnings = st.session_state.get("quality_warnings", {})
    display_df = display_df.copy()
    display_df["품질"] = display_df["KOL명"].apply(
        lambda name: f"⚠️ {len(quality_warnings[name])}건" if name in quality_warnings else "✅"
    )

    # 컬럼 색상 스타일 적용
    def _color_grade(val):
        colors = {
            "★★★★★": "background-color:#d5ead0; font-weight:800",
            "★★★★":  "background-color:#d5e8f0; font-weight:700",
            "★★★":   "background-color:#fffdd0",
            "★★":    "background-color:#ffe5cc",
            "★":     "background-color:#ffe0e0",
        }
        return colors.get(str(val), "")

    def _color_adopt(val):
        val = str(val)
        if "최우선" in val or "채택 권고" in val:
            return "color:#1a7a1a; font-weight:700"
        if "조건부" in val:
            return "color:#8a6a00"
        if "보류" in val:
            return "color:#cc8800"
        if "비권고" in val:
            return "color:#cc0000"
        return ""

    # 표시할 컬럼만 선택
    show_cols = [
        "KOL명", "플랫폼", "품질", "비용(JPY)", "평균 조회수", "평균 좋아요",
        "평균 댓글", "평균 저장", "평균 공유",
        "CPV(¥/회)", "ER%", "저장률%", "CPE(¥/건)",
        "저장비율%", "댓글비율%", "종합점수", "등급", "채택권고"
    ]
    show_cols = [c for c in show_cols if c in display_df.columns]

    styled = (
        display_df[show_cols]
        .sort_values("종합점수", ascending=False, na_position="last")
        .style
        .applymap(_color_grade, subset=["등급"])
        .applymap(_color_adopt, subset=["채택권고"])
        .format({
            "비용(JPY)":   lambda x: f"¥{int(x):,}" if pd.notna(x) else "—",
            "평균 조회수": lambda x: f"{int(x):,}"  if pd.notna(x) else "—",
            "평균 좋아요": lambda x: f"{int(x):,}"  if pd.notna(x) else "—",
            "평균 댓글":   lambda x: f"{int(x):,}"  if pd.notna(x) else "—",
            "평균 저장":   lambda x: f"{int(x):,}"  if pd.notna(x) else "—",
            "평균 공유":   lambda x: f"{int(x):,}"  if pd.notna(x) else "—",
            "CPV(¥/회)":  lambda x: f"¥{x:.2f}"    if pd.notna(x) else "—",
            "ER%":         lambda x: f"{x:.2f}%"    if pd.notna(x) else "—",
            "저장률%":     lambda x: f"{x:.2f}%"    if pd.notna(x) else "—",
            "CPE(¥/건)":  lambda x: f"¥{x:.0f}"    if pd.notna(x) else "—",
            "저장비율%":   lambda x: f"{x:.1f}%"    if pd.notna(x) else "—",
            "댓글비율%":   lambda x: f"{x:.1f}%"    if pd.notna(x) else "—",
            "종합점수":    lambda x: f"{x:.1f}"      if pd.notna(x) else "—",
        }, na_rep="—")
    )

    st.dataframe(styled, use_container_width=True, height=400)

    # ── 오디언스 품질 경고 상세 ──
    if quality_warnings:
        with st.expander(f"🔍 오디언스 품질 경고 ({len(quality_warnings)}명)", expanded=False):
            for kol_name, flags in quality_warnings.items():
                st.markdown(f"**{kol_name}**")
                for flag in flags:
                    st.markdown(f"- {flag}")
                st.markdown("")
    else:
        if st.session_state.get("quality_warnings") is not None:
            st.success("✅ 품질 경고 없음 — 모든 KOL이 정상 범위입니다.")

    # ── Excel 다운로드 ──
    st.markdown("---")
    st.markdown("### 📥 Excel 스코어카드 다운로드")

    col_dl1, col_dl2 = st.columns([1, 3])
    with col_dl1:
        try:
            excel_bytes = generate_excel_scorecard(df_result)
            st.download_button(
                label="📊 Excel 다운로드",
                data=excel_bytes,
                file_name="KOL_스코어카드.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                type="primary"
            )
        except Exception as e:
            st.error(f"Excel 생성 오류: {e}")

    with col_dl2:
        st.info(
            "📋 Excel 파일에는 **3개 시트**가 포함됩니다:\n"
            "1. KOL 스코어카드 (전체 지표 + 점수)\n"
            "2. 플랫폼별 랭킹\n"
            "3. 벤치마크 기준"
        )

# ──────────────────────────────────────────────────────────────
#  F1: 자사/타사 콘텐츠 실시간 탐색
# ──────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("### 🔎 콘텐츠 실시간 탐색 (F1)")
st.caption("브랜드명을 입력하면 TikTok·Instagram·YouTube에서 해당 브랜드 관련 콘텐츠를 수집합니다.")

f1_col1, f1_col2 = st.columns([2, 2])
with f1_col1:
    f1_own_brand = st.text_input("자사 브랜드명", placeholder="예: ANESSA", key="f1_own")
with f1_col2:
    f1_competitors = st.text_input("경쟁사 브랜드 (쉼표 구분)", placeholder="예: Biore, Skin Aqua", key="f1_comp")

f1_platforms = st.multiselect(
    "검색 플랫폼",
    ["TikTok", "Instagram", "YouTube"],
    default=["TikTok", "YouTube"],
    key="f1_plat"
)
f1_max = st.slider("브랜드당 최대 수집 수", 10, 50, 30, key="f1_max")

if st.button("🔍 콘텐츠 탐색 시작", type="primary", use_container_width=True, key="f1_run"):
    brands = []
    if f1_own_brand.strip():
        brands.append(f1_own_brand.strip())
    for comp in f1_competitors.split(","):
        if comp.strip():
            brands.append(comp.strip())

    if not brands:
        st.warning("브랜드명을 입력하세요.")
    elif not f1_platforms:
        st.warning("플랫폼을 선택하세요.")
    else:
        f1_progress = st.progress(0, text="탐색 준비 중...")
        browser_dir = Path.home() / ".kol_tool_session"

        pw_ctx = None
        _pw = None
        if not IS_CLOUD:
            try:
                from playwright.sync_api import sync_playwright
                _pw = sync_playwright().__enter__()
                if browser_dir.exists():
                    pw_ctx = _pw.chromium.launch_persistent_context(str(browser_dir), headless=True)
            except Exception:
                pass

        all_brand_posts = []
        for bi, brand in enumerate(brands):
            f1_progress.progress(
                (bi + 0.5) / len(brands),
                text=f"[{bi+1}/{len(brands)}] '{brand}' 검색 중..."
            )
            pw_page = None
            if pw_ctx:
                try:
                    pw_page = pw_ctx.new_page()
                except Exception:
                    pass

            posts = search_brand(brand, f1_platforms, f1_max, pw_page)
            all_brand_posts.extend(posts)

            if pw_page:
                try:
                    pw_page.close()
                except Exception:
                    pass

        if pw_ctx:
            try:
                pw_ctx.close()
                _pw.__exit__(None, None, None)
            except Exception:
                pass

        f1_progress.progress(1.0, text="✅ 탐색 완료!")
        st.session_state.f1_results = all_brand_posts

# F1 결과 표시
if st.session_state.get("f1_results"):
    f1_data = st.session_state.f1_results
    f1_df = pd.DataFrame(f1_data)

    if not f1_df.empty:
        # 브랜드별 콘텐츠 목록 테이블
        st.markdown("#### 📋 브랜드별 콘텐츠 목록")
        brand_filter = st.multiselect(
            "브랜드 필터", f1_df["brand"].unique().tolist(),
            default=f1_df["brand"].unique().tolist(), key="f1_brand_filter"
        )
        filtered_f1 = f1_df[f1_df["brand"].isin(brand_filter)] if brand_filter else f1_df

        display_f1 = filtered_f1[["brand", "kol_name", "platform", "views", "likes",
                                   "comments", "saves", "url"]].rename(columns={
            "brand": "브랜드", "kol_name": "KOL명", "platform": "플랫폼",
            "views": "조회수", "likes": "좋아요", "comments": "댓글",
            "saves": "저장", "url": "URL"
        }).sort_values("조회수", ascending=False, na_position="last")

        st.dataframe(display_f1, use_container_width=True, hide_index=True, height=300)

        # 상위 바이럴 Top 10
        top10 = filtered_f1.dropna(subset=["views"]).nlargest(10, "views")
        if not top10.empty:
            st.markdown("#### 🔥 바이럴 콘텐츠 Top 10")
            for idx, (_, row) in enumerate(top10.iterrows()):
                views_str = f"{int(row['views']):,}" if row.get('views') else "—"
                st.markdown(
                    f"**{idx+1}.** {row.get('kol_name', '—')} ({row['platform']}) — "
                    f"조회수 {views_str} | [{row['url']}]({row['url']})"
                )

        # 플랫폼별 평균 ER
        st.markdown("#### 📊 플랫폼별 평균 참여율")
        er_data = []
        for plat in filtered_f1["platform"].unique():
            plat_posts = filtered_f1[filtered_f1["platform"] == plat]
            views_col = plat_posts["views"].dropna()
            likes_col = plat_posts["likes"].dropna()
            comments_col = plat_posts["comments"].dropna()
            saves_col = plat_posts["saves"].dropna()
            total_views = views_col.sum()
            total_eng = likes_col.sum() + comments_col.sum() + saves_col.sum()
            er = round(total_eng / total_views * 100, 2) if total_views > 0 else None
            er_data.append({
                "플랫폼": plat, "총 조회수": int(total_views),
                "총 참여": int(total_eng), "평균 ER%": er,
                "콘텐츠 수": len(plat_posts),
            })
        if er_data:
            st.dataframe(pd.DataFrame(er_data), use_container_width=True, hide_index=True)

        # 해시태그 Top 20
        all_tags = []
        for tags in filtered_f1["hashtags"].dropna():
            if isinstance(tags, list):
                all_tags.extend(tags)
        if all_tags:
            st.markdown("#### #️⃣ 해시태그 Top 20")
            from collections import Counter
            tag_counts = Counter(all_tags).most_common(20)
            tag_df = pd.DataFrame(tag_counts, columns=["해시태그", "출현 횟수"])
            st.dataframe(tag_df, use_container_width=True, hide_index=True)

        # ──────────────────────────────────────────────────────
        #  F2: 인사이트 도출 + 벤치마크 보정
        # ──────────────────────────────────────────────────────
        st.markdown("---")
        st.markdown("### 💡 인사이트 & 벤치마크 보정 (F2)")

        brands_in_data = filtered_f1["brand"].unique().tolist()
        own_brand = brands_in_data[0] if brands_in_data else ""

        # 자사 vs 경쟁사 ER 비교
        if len(brands_in_data) >= 2:
            st.markdown("#### 자사 vs 경쟁사 평균 ER 비교")
            brand_er = []
            for b in brands_in_data:
                b_posts = filtered_f1[filtered_f1["brand"] == b]
                b_views = b_posts["views"].dropna().sum()
                b_eng = (b_posts["likes"].dropna().sum() +
                         b_posts["comments"].dropna().sum() +
                         b_posts["saves"].dropna().sum())
                b_er = round(b_eng / b_views * 100, 2) if b_views > 0 else None
                label = f"🏠 {b}" if b == own_brand else b
                brand_er.append({"브랜드": label, "평균 ER%": b_er, "콘텐츠 수": len(b_posts)})
            brand_er_df = pd.DataFrame(brand_er)
            st.dataframe(brand_er_df, use_container_width=True, hide_index=True)
            # 바 차트
            chart_data = brand_er_df.dropna(subset=["평균 ER%"])
            if not chart_data.empty:
                st.bar_chart(chart_data.set_index("브랜드"), y="평균 ER%", use_container_width=True)

        # 바이럴 패턴 분석
        st.markdown("#### 🧩 바이럴 콘텐츠 패턴")
        if all_tags:
            st.markdown(f"- **주요 해시태그**: {', '.join(t for t, _ in tag_counts[:10])}")
        plat_dist = filtered_f1["platform"].value_counts()
        st.markdown(f"- **플랫폼 분포**: {' / '.join(f'{p} {c}건' for p, c in plat_dist.items())}")
        total_posts = len(filtered_f1)
        viral_threshold = filtered_f1["views"].dropna().quantile(0.9) if len(filtered_f1["views"].dropna()) > 5 else None
        if viral_threshold:
            st.markdown(f"- **상위 10% 조회수 기준**: {int(viral_threshold):,}회 이상")

        # 벤치마크 보정
        st.markdown("#### 📐 벤치마크 보정")
        st.caption("F1 수집 데이터 기반으로 플랫폼별 카테고리 평균 ER을 자동 계산합니다.")

        benchmark_data = []
        from scorer import PLATFORM_WEIGHTS
        for plat in filtered_f1["platform"].unique():
            plat_posts = filtered_f1[filtered_f1["platform"] == plat]
            v = plat_posts["views"].dropna()
            eng = (plat_posts["likes"].dropna().sum() +
                   plat_posts["comments"].dropna().sum() +
                   plat_posts["saves"].dropna().sum())
            market_er = round(eng / v.sum() * 100, 2) if v.sum() > 0 else None
            benchmark_data.append({
                "플랫폼": plat,
                "시장 평균 ER%": market_er,
                "샘플 수": len(plat_posts),
            })

        if benchmark_data:
            bm_df = pd.DataFrame(benchmark_data)
            st.dataframe(bm_df, use_container_width=True, hide_index=True)

            # 보정 이력 저장
            if "benchmark_history" not in st.session_state:
                st.session_state.benchmark_history = []

            if st.button("📌 벤치마크 보정값 저장", key="f2_save_bm"):
                from datetime import datetime
                entry = {
                    "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "benchmarks": benchmark_data,
                }
                st.session_state.benchmark_history.append(entry)
                st.success("✅ 벤치마크 보정값이 저장되었습니다.")

            if st.session_state.get("benchmark_history"):
                with st.expander("📜 보정 이력"):
                    for entry in reversed(st.session_state.benchmark_history):
                        st.markdown(f"**{entry['date']}**")
                        st.dataframe(
                            pd.DataFrame(entry["benchmarks"]),
                            use_container_width=True, hide_index=True
                        )


# ──────────────────────────────────────────────────────────────
#  KOL 히스토리 DB
# ──────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("### 📂 KOL 히스토리 DB")

db_stats = get_db_stats()
if db_stats["kol_count"] == 0:
    st.info("아직 저장된 KOL이 없습니다. 스코어카드를 실행하면 자동으로 DB에 저장됩니다.")
else:
    # 통계 요약
    stat_cols = st.columns(3)
    with stat_cols[0]:
        st.metric("등록 KOL 수", f"{db_stats['kol_count']}명")
    with stat_cols[1]:
        st.metric("스냅샷 기록", f"{db_stats['snapshot_count']}건")
    with stat_cols[2]:
        plat_summary = " / ".join(f"{p} {c}명" for p, c in db_stats["platforms"].items())
        st.metric("플랫폼 분포", plat_summary)

    all_kols = get_all_kols()
    kol_db_df = pd.DataFrame(all_kols)

    # 필터
    filter_cols = st.columns([2, 2, 2])
    with filter_cols[0]:
        plat_filter = st.multiselect(
            "플랫폼 필터",
            options=sorted(kol_db_df["platform"].unique().tolist()),
            default=[],
            key="db_plat_filter"
        )
    with filter_cols[1]:
        CAMPAIGN_STATUSES = ["미접촉", "컨택 중", "진행 중", "완료", "보류"]
        status_filter = st.multiselect(
            "캠페인 상태",
            options=CAMPAIGN_STATUSES,
            default=[],
            key="db_status_filter"
        )
    with filter_cols[2]:
        search_query = st.text_input("KOL명 검색", key="db_search")

    filtered = kol_db_df.copy()
    if plat_filter:
        filtered = filtered[filtered["platform"].isin(plat_filter)]
    if status_filter:
        filtered = filtered[filtered["campaign_status"].isin(status_filter)]
    if search_query:
        filtered = filtered[filtered["name"].str.contains(search_query, case=False, na=False)]

    if filtered.empty:
        st.info("조건에 맞는 KOL이 없습니다.")
    else:
        # 표시용 DataFrame 구성
        display_kols = filtered.rename(columns={
            "name": "KOL명", "platform": "플랫폼", "url": "URL",
            "campaign_status": "캠페인 상태",
            "latest_score": "최신 점수", "latest_grade": "등급",
            "latest_er": "ER%", "latest_views": "평균 조회수",
            "first_scored_at": "최초 등록", "last_updated_at": "마지막 업데이트",
            "memo": "메모",
        })

        show_db_cols = ["KOL명", "플랫폼", "캠페인 상태", "최신 점수", "등급",
                        "ER%", "평균 조회수", "최초 등록", "마지막 업데이트", "메모"]
        show_db_cols = [c for c in show_db_cols if c in display_kols.columns]

        st.dataframe(
            display_kols[show_db_cols].sort_values("최신 점수", ascending=False, na_position="last"),
            use_container_width=True,
            height=300,
            hide_index=True,
        )

        # KOL 상세 보기
        kol_names = filtered["name"].tolist()
        selected_kol = st.selectbox("KOL 상세 보기", ["선택하세요"] + kol_names, key="db_detail")

        if selected_kol != "선택하세요":
            kol_row = filtered[filtered["name"] == selected_kol].iloc[0]
            kol_id = int(kol_row["id"])

            detail_cols = st.columns([2, 1])
            with detail_cols[0]:
                st.markdown(f"**{selected_kol}** ({kol_row['platform']}) — [{kol_row['url']}]({kol_row['url']})")

                # 이전 대비 변화
                delta = get_kol_delta(kol_id)
                if delta:
                    delta_items = []
                    for key, label in [("score", "점수"), ("er_pct", "ER%"), ("avg_views", "조회수")]:
                        d = delta.get(key)
                        if d is not None:
                            sign = "+" if d > 0 else ""
                            delta_items.append(f"{label}: {sign}{d}")
                    if delta_items:
                        st.caption(f"지난 측정 대비 변화: {' | '.join(delta_items)}")

                # 스냅샷 이력
                history = get_kol_history(kol_id)
                if history:
                    hist_df = pd.DataFrame(history)
                    hist_show = hist_df[["snapshot_at", "score", "grade", "er_pct",
                                         "avg_views", "cpv", "fee"]].rename(columns={
                        "snapshot_at": "측정일", "score": "점수", "grade": "등급",
                        "er_pct": "ER%", "avg_views": "평균 조회수",
                        "cpv": "CPV", "fee": "비용(JPY)"
                    })
                    st.dataframe(hist_show, use_container_width=True, hide_index=True, height=200)

            with detail_cols[1]:
                # 캠페인 상태 변경
                current_status = kol_row.get("campaign_status", "미접촉")
                new_status = st.selectbox(
                    "캠페인 상태 변경",
                    CAMPAIGN_STATUSES,
                    index=CAMPAIGN_STATUSES.index(current_status) if current_status in CAMPAIGN_STATUSES else 0,
                    key=f"status_{kol_id}"
                )
                if new_status != current_status:
                    if st.button("상태 저장", key=f"save_status_{kol_id}"):
                        update_campaign_status(kol_id, new_status)
                        st.success(f"✅ {selected_kol} → {new_status}")
                        st.rerun()

                # 메모
                current_memo = kol_row.get("memo", "") or ""
                new_memo = st.text_area("메모", value=current_memo, key=f"memo_{kol_id}", height=80)
                if new_memo != current_memo:
                    if st.button("메모 저장", key=f"save_memo_{kol_id}"):
                        update_memo(kol_id, new_memo)
                        st.success("✅ 메모 저장됨")
                        st.rerun()


# ──────────────────────────────────────────────────────────────
#  F3: KOL 자동 발굴 및 컨택 리스트
# ──────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("### 🔍 KOL 자동 발굴 (F3)")
st.caption("키워드/해시태그로 KOL 후보를 탐색하고, 자동 스코어링 후 컨택 리스트를 생성합니다.")

f3_col1, f3_col2 = st.columns([2, 1])
with f3_col1:
    f3_keywords = st.text_input(
        "검색 키워드/해시태그",
        placeholder="예: #선크림, #쿠션파운데이션, 日焼け止め",
        key="f3_keywords"
    )
with f3_col2:
    f3_top_n = st.number_input("발굴 인원 수", min_value=5, max_value=100, value=30, key="f3_topn")

f3_col3, f3_col4 = st.columns([2, 2])
with f3_col3:
    f3_platforms = st.multiselect(
        "플랫폼",
        ["TikTok", "Instagram", "YouTube"],
        default=["TikTok"],
        key="f3_plat"
    )
with f3_col4:
    f3_fee_estimate = st.number_input(
        "예상 단가(JPY, 0이면 CPV 미산출)",
        min_value=0, value=0, step=10000, key="f3_fee"
    )

if st.button("🚀 KOL 발굴 시작", type="primary", use_container_width=True, key="f3_run"):
    keywords = [k.strip() for k in re.split(r"[,、]", f3_keywords) if k.strip()]
    if not keywords:
        st.warning("키워드를 입력하세요.")
    elif not f3_platforms:
        st.warning("플랫폼을 선택하세요.")
    else:
        f3_progress = st.progress(0, text="KOL 후보 탐색 중...")
        browser_dir = Path.home() / ".kol_tool_session"

        pw_ctx = None
        _pw = None
        if not IS_CLOUD:
            try:
                from playwright.sync_api import sync_playwright
                _pw = sync_playwright().__enter__()
                if browser_dir.exists():
                    pw_ctx = _pw.chromium.launch_persistent_context(str(browser_dir), headless=True)
            except Exception:
                pass

        # Step 1: 키워드 검색으로 후보 수집
        all_search_posts = []
        for ki, kw in enumerate(keywords):
            f3_progress.progress(
                (ki + 0.3) / (len(keywords) + 1),
                text=f"'{kw}' 검색 중..."
            )
            pw_page = None
            if pw_ctx:
                try:
                    pw_page = pw_ctx.new_page()
                except Exception:
                    pw_page = None
            posts = search_brand(kw, f3_platforms, f3_top_n * 2, pw_page)
            all_search_posts.extend(posts)
            if pw_page:
                try:
                    pw_page.close()
                except Exception:
                    pass

        # Step 2: 고유 KOL 후보 추출
        candidates = extract_kol_candidates(all_search_posts)[:f3_top_n]
        f3_progress.progress(0.6, text=f"{len(candidates)}명 후보 발견. 스코어링 중...")

        # Step 3: 각 후보 프로필 스크래핑 + 점수 계산
        kol_list = []
        for ci, cand in enumerate(candidates):
            f3_progress.progress(
                0.6 + 0.4 * (ci + 0.5) / len(candidates),
                text=f"[{ci+1}/{len(candidates)}] {cand['kol_name']} 스크래핑..."
            )
            pw_page = None
            if pw_ctx:
                try:
                    pw_page = pw_ctx.new_page()
                except Exception:
                    pw_page = None

            raw = scrape_kol(
                url=cand["profile_url"],
                num_posts=12,
                pinned_ids=[],
                playwright_page=pw_page
            )

            if pw_page:
                try:
                    pw_page.close()
                except Exception:
                    pass

            fee = float(f3_fee_estimate) if f3_fee_estimate else None
            kol = KOLMetrics(
                name=cand["kol_name"],
                platform=cand["platform"],
                url=cand["profile_url"],
                fee=fee,
                avg_views=raw.get("avg_views"),
                avg_likes=raw.get("avg_likes"),
                avg_comments=raw.get("avg_comments"),
                avg_saves=raw.get("avg_saves"),
                avg_shares=raw.get("avg_shares"),
                post_count=raw.get("post_count", 0),
            )
            kol_list.append(kol)
            time.sleep(0.3)

        if pw_ctx:
            try:
                pw_ctx.close()
                _pw.__exit__(None, None, None)
            except Exception:
                pass

        # 점수 계산
        if kol_list:
            scored = calculate_all_scores(kol_list)
            contact_df = kols_to_dataframe(scored)
            # DB에도 저장
            save_scored_df(contact_df)
            st.session_state.f3_contact_list = contact_df

        f3_progress.progress(1.0, text="✅ KOL 발굴 완료!")

# F3 결과 표시
if st.session_state.get("f3_contact_list") is not None:
    contact_df = st.session_state.f3_contact_list

    st.markdown("#### 📋 컨택 리스트")
    st.caption(f"총 {len(contact_df)}명 | 종합점수 기준 정렬")

    f3_show_cols = ["KOL명", "플랫폼", "평균 조회수", "평균 좋아요",
                    "ER%", "CPV(¥/회)", "종합점수", "등급", "채택권고", "URL"]
    f3_show_cols = [c for c in f3_show_cols if c in contact_df.columns]

    f3_display = (contact_df[f3_show_cols]
                  .sort_values("종합점수", ascending=False, na_position="last"))
    st.dataframe(f3_display, use_container_width=True, hide_index=True, height=400)

    # Excel 다운로드
    try:
        f3_buf = io.BytesIO()
        with pd.ExcelWriter(f3_buf, engine="openpyxl") as writer:
            f3_display.to_excel(writer, sheet_name="컨택 리스트", index=False)
        f3_buf.seek(0)
        st.download_button(
            "📥 컨택 리스트 Excel 다운로드",
            data=f3_buf.getvalue(),
            file_name="KOL_컨택리스트.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key="f3_download"
        )
    except Exception as e:
        st.error(f"Excel 생성 오류: {e}")


# ──────────────────────────────────────────────────────────────
#  A3: 캠페인 예산 시뮬레이터
# ──────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("### 💰 캠페인 예산 시뮬레이터 (A3)")
st.caption("예산을 입력하면 KOL DB 기반으로 최적 KOL 조합을 추천합니다.")

a3_kols_raw = get_all_kols()
# 비용과 조회수가 있는 KOL만 필터
a3_eligible = [
    k for k in a3_kols_raw
    if k.get("latest_fee") and k.get("latest_views")
    and k["latest_fee"] > 0 and k["latest_views"] > 0
]

if not a3_eligible:
    st.info("DB에 비용과 조회수 데이터가 있는 KOL이 없습니다. 스코어카드를 먼저 실행하세요.")
else:
    a3_col1, a3_col2 = st.columns([1, 1])
    with a3_col1:
        a3_budget = st.number_input(
            "총 예산 (JPY)", min_value=10000, value=500000, step=50000,
            format="%d", key="a3_budget"
        )
    with a3_col2:
        a3_goal = st.selectbox(
            "최적화 목표",
            ["총 노출 최대화", "총 인게이지먼트 최대화", "CPV 최소화"],
            key="a3_goal"
        )

    # 플랫폼 필터
    a3_plats = list(set(k["platform"] for k in a3_eligible))
    a3_plat_filter = st.multiselect(
        "플랫폼 필터 (비우면 전체)",
        a3_plats, default=[], key="a3_plat_filter"
    )
    if a3_plat_filter:
        a3_eligible = [k for k in a3_eligible if k["platform"] in a3_plat_filter]

    if st.button("🧮 최적 조합 계산", type="primary", use_container_width=True, key="a3_run"):
        # 배낭 문제 (0/1 knapsack) — 동적 프로그래밍
        # 비용 단위를 1000JPY로 스케일링
        SCALE = 1000
        capacity = int(a3_budget) // SCALE
        n = len(a3_eligible)

        # 각 KOL의 가치 계산
        items = []
        for k in a3_eligible:
            fee = int(k["latest_fee"]) // SCALE
            if fee <= 0:
                fee = 1
            views = k.get("latest_views") or 0
            er = k.get("latest_er") or 0
            engagement = views * er / 100 if er else 0

            if a3_goal == "총 노출 최대화":
                value = int(views)
            elif a3_goal == "총 인게이지먼트 최대화":
                value = int(engagement)
            else:  # CPV 최소화 → 가치 = 조회수/비용 (효율)
                value = int(views / fee) if fee > 0 else 0

            items.append({
                "data": k,
                "fee_scaled": fee,
                "value": value,
            })

        # DP 테이블
        dp = [0] * (capacity + 1)
        keep = [[False] * (capacity + 1) for _ in range(n)]

        for i in range(n):
            w = items[i]["fee_scaled"]
            v = items[i]["value"]
            for c in range(capacity, w - 1, -1):
                if dp[c - w] + v > dp[c]:
                    dp[c] = dp[c - w] + v
                    keep[i][c] = True

        # 역추적으로 선택된 KOL 찾기
        selected = []
        c = capacity
        for i in range(n - 1, -1, -1):
            if keep[i][c]:
                selected.append(items[i])
                c -= items[i]["fee_scaled"]

        if not selected:
            st.warning("예산 내에서 선택 가능한 KOL이 없습니다. 예산을 늘려보세요.")
        else:
            st.session_state.a3_result = selected

    # 결과 표시
    if st.session_state.get("a3_result"):
        selected = st.session_state.a3_result

        total_cost = sum(s["data"]["latest_fee"] for s in selected)
        total_views = sum(s["data"].get("latest_views") or 0 for s in selected)
        total_value = sum(s["value"] for s in selected)

        st.markdown("#### 📊 추천 KOL 조합")
        m_cols = st.columns(4)
        with m_cols[0]:
            st.metric("선택 KOL 수", f"{len(selected)}명")
        with m_cols[1]:
            st.metric("예상 총 비용", f"¥{int(total_cost):,}")
        with m_cols[2]:
            st.metric("예상 총 노출수", f"{int(total_views):,}")
        with m_cols[3]:
            avg_cpv = total_cost / total_views if total_views > 0 else 0
            st.metric("예상 평균 CPV", f"¥{avg_cpv:.2f}")

        sim_rows = []
        for s in selected:
            k = s["data"]
            fee = k["latest_fee"]
            views = k.get("latest_views") or 0
            contribution = round(views / total_views * 100, 1) if total_views > 0 else 0
            sim_rows.append({
                "KOL명": k["name"],
                "플랫폼": k["platform"],
                "비용(JPY)": int(fee),
                "예상 조회수": int(views),
                "점수": k.get("latest_score"),
                "등급": k.get("latest_grade", "—"),
                "ER%": k.get("latest_er"),
                "기여도%": contribution,
            })
        sim_df = pd.DataFrame(sim_rows).sort_values("기여도%", ascending=False)
        st.dataframe(sim_df, use_container_width=True, hide_index=True)

        # 기여도 차트
        st.markdown("#### KOL별 예상 기여도")
        chart_df = sim_df[["KOL명", "기여도%"]].set_index("KOL명")
        st.bar_chart(chart_df, y="기여도%", use_container_width=True)

        # 잔여 예산
        remaining = int(a3_budget) - int(total_cost)
        if remaining > 0:
            st.info(f"잔여 예산: ¥{remaining:,}")

        # Excel 다운로드
        try:
            a3_buf = io.BytesIO()
            with pd.ExcelWriter(a3_buf, engine="openpyxl") as writer:
                sim_df.to_excel(writer, sheet_name="예산 시뮬레이션", index=False)
            a3_buf.seek(0)
            st.download_button(
                "📥 시뮬레이션 결과 Excel",
                data=a3_buf.getvalue(),
                file_name="KOL_예산_시뮬레이션.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key="a3_download"
            )
        except Exception as e:
            st.error(f"Excel 오류: {e}")


# ──────────────────────────────────────────────────────────────
#  F5: 유가 포스팅 성과 분석
# ──────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("### 💰 유가 포스팅 성과 분석")
st.caption("실제 집행한 유료 KOL 포스팅의 사후 성과를 측정합니다.")

f5_tab_input, f5_tab_report = st.tabs(["📝 포스팅 등록", "📊 성과 리포트"])

with f5_tab_input:
    # 캠페인 선택/생성
    campaigns = get_campaigns()
    campaign_names = [c["campaign_name"] for c in campaigns]

    f5_col1, f5_col2 = st.columns([2, 1])
    with f5_col1:
        new_campaign = st.text_input("새 캠페인 생성", placeholder="예: 2024 봄 선크림 캠페인")
    with f5_col2:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("➕ 캠페인 생성", use_container_width=True) and new_campaign.strip():
            cid = create_campaign(new_campaign.strip())
            st.success(f"✅ 캠페인 '{new_campaign}' 생성됨")
            st.rerun()

    if not campaigns:
        st.info("먼저 캠페인을 생성하세요.")
    else:
        selected_campaign_name = st.selectbox(
            "캠페인 선택",
            campaign_names,
            key="f5_campaign_select"
        )
        selected_campaign = next(c for c in campaigns if c["campaign_name"] == selected_campaign_name)
        sel_cid = selected_campaign["id"]

        # 입력 방식 선택
        f5_manual, f5_csv = st.tabs(["✏️ 개별 입력", "📂 CSV 업로드"])

        with f5_manual:
            CONTENT_TYPES = ["", "언박싱", "데일리 루틴", "비포애프터", "튜토리얼", "리뷰", "이벤트·챌린지", "기타"]

            if "f5_rows" not in st.session_state:
                st.session_state.f5_rows = pd.DataFrame({
                    "KOL명": [""], "URL": [""], "비용(JPY)": [0],
                    "포스팅일": [""], "소재유형": [""],
                })

            f5_add_col, f5_clear_col = st.columns([1, 1])
            with f5_add_col:
                if st.button("➕ 행 추가", key="f5_add_row", use_container_width=True):
                    new_r = pd.DataFrame({
                        "KOL명": [""], "URL": [""], "비용(JPY)": [0],
                        "포스팅일": [""], "소재유형": [""],
                    })
                    st.session_state.f5_rows = pd.concat(
                        [st.session_state.f5_rows, new_r], ignore_index=True
                    )
            with f5_clear_col:
                if st.button("🗑 초기화", key="f5_clear", use_container_width=True):
                    st.session_state.f5_rows = pd.DataFrame({
                        "KOL명": [""], "URL": [""], "비용(JPY)": [0],
                        "포스팅일": [""], "소재유형": [""],
                    })

            f5_edited = st.data_editor(
                st.session_state.f5_rows,
                use_container_width=True,
                num_rows="dynamic",
                column_config={
                    "KOL명": st.column_config.TextColumn("KOL명", width="medium"),
                    "URL": st.column_config.TextColumn("포스팅 URL (필수)", width="large"),
                    "비용(JPY)": st.column_config.NumberColumn("지급 비용(JPY)", min_value=0, step=1000, format="¥%d"),
                    "포스팅일": st.column_config.TextColumn("포스팅 날짜", width="medium",
                                                           help="YYYY-MM-DD 형식"),
                    "소재유형": st.column_config.SelectboxColumn("소재 유형", options=CONTENT_TYPES, width="medium"),
                },
                hide_index=True,
                key="f5_editor",
            )
            st.session_state.f5_rows = f5_edited

        with f5_csv:
            st.caption("필수 열: **URL**, **비용(JPY)**  /  선택 열: KOL명, 포스팅일, 소재유형")
            f5_uploaded = st.file_uploader("CSV/Excel 업로드", type=["csv", "xlsx"], key="f5_upload")
            if f5_uploaded:
                try:
                    if f5_uploaded.name.endswith(".csv"):
                        f5_df_up = pd.read_csv(f5_uploaded)
                    else:
                        f5_df_up = pd.read_excel(f5_uploaded)

                    col_map = {}
                    for c in f5_df_up.columns:
                        cl = str(c).strip().lower()
                        if "url" in cl:
                            col_map[c] = "URL"
                        elif "kol" in cl or "name" in cl or "명" in cl:
                            col_map[c] = "KOL명"
                        elif "비용" in cl or "fee" in cl or "cost" in cl:
                            col_map[c] = "비용(JPY)"
                        elif "날짜" in cl or "date" in cl or "일" in cl:
                            col_map[c] = "포스팅일"
                        elif "소재" in cl or "type" in cl or "유형" in cl:
                            col_map[c] = "소재유형"
                    f5_df_up = f5_df_up.rename(columns=col_map)

                    for col in ["KOL명", "비용(JPY)", "포스팅일", "소재유형"]:
                        if col not in f5_df_up.columns:
                            f5_df_up[col] = "" if col != "비용(JPY)" else 0

                    st.session_state.f5_rows = f5_df_up[["KOL명", "URL", "비용(JPY)", "포스팅일", "소재유형"]]
                    st.success(f"✅ {len(f5_df_up)}건 로드됨")
                except Exception as e:
                    st.error(f"파일 오류: {e}")

        # 등록 + 스크래핑 실행
        f5_valid = f5_edited[f5_edited["URL"].str.strip().str.len() > 5] if not f5_edited.empty else pd.DataFrame()

        if not f5_valid.empty:
            st.markdown(f"**{len(f5_valid)}건** 등록 대기")
            if st.button("🚀 등록 + 성과 스크래핑", type="primary", use_container_width=True, key="f5_run"):
                f5_progress = st.progress(0, text="유가 포스팅 스크래핑 준비 중...")
                browser_dir = Path.home() / ".kol_tool_session"

                pw_ctx = None
                _pw = None
                try:
                    from playwright.sync_api import sync_playwright
                    _pw = sync_playwright().__enter__()
                    if browser_dir.exists():
                        pw_ctx = _pw.chromium.launch_persistent_context(str(browser_dir), headless=True)
                except Exception:
                    pass

                f5_results = []
                for i, (_, row) in enumerate(f5_valid.iterrows()):
                    url = str(row["URL"]).strip()
                    plat = detect_platform(url)
                    kol_name_raw = str(row.get("KOL명", "")).strip()
                    if not kol_name_raw or kol_name_raw in ("None", "nan"):
                        kol_name_raw = extract_username(url, plat) or url
                    fee = float(row.get("비용(JPY)", 0) or 0)
                    post_date = str(row.get("포스팅일", "")).strip()
                    content_type = str(row.get("소재유형", "")).strip()

                    f5_progress.progress(
                        (i + 0.5) / len(f5_valid),
                        text=f"[{i+1}/{len(f5_valid)}] {kol_name_raw} 스크래핑 중..."
                    )

                    # DB에 포스팅 등록
                    post_id = add_paid_post(
                        sel_cid, kol_name_raw, plat, url,
                        post_date if post_date not in ("", "nan", "None") else "",
                        fee, content_type
                    )

                    # 스크래핑
                    pw_page = None
                    if pw_ctx:
                        try:
                            pw_page = pw_ctx.new_page()
                        except Exception:
                            pw_page = None

                    raw = scrape_kol(url=url, num_posts=1, pinned_ids=[], playwright_page=pw_page)

                    if pw_page:
                        try:
                            pw_page.close()
                        except Exception:
                            pass

                    if raw["success"]:
                        views = raw.get("avg_views") or 0
                        likes = raw.get("avg_likes") or 0
                        comments_val = raw.get("avg_comments") or 0
                        saves_val = raw.get("avg_saves") or 0
                        shares_val = raw.get("avg_shares") or 0

                        calc = update_paid_post_metrics(
                            post_id, views, likes, comments_val, saves_val, shares_val, fee
                        )
                        f5_results.append({
                            "KOL명": kol_name_raw, "플랫폼": plat, "상태": "✅",
                            "조회수": views, "CPV": calc.get("cpv"), "CPE": calc.get("cpe"),
                            "ER%": calc.get("er_pct"),
                        })
                    else:
                        f5_results.append({
                            "KOL명": kol_name_raw, "플랫폼": plat, "상태": "❌",
                            "조회수": None, "CPV": None, "CPE": None, "ER%": None,
                        })
                    time.sleep(0.3)

                if pw_ctx:
                    try:
                        pw_ctx.close()
                        _pw.__exit__(None, None, None)
                    except Exception:
                        pass

                f5_progress.progress(1.0, text="✅ 완료!")
                st.dataframe(pd.DataFrame(f5_results), use_container_width=True, hide_index=True)

with f5_tab_report:
    campaigns_for_report = get_campaigns()
    if not campaigns_for_report:
        st.info("등록된 캠페인이 없습니다.")
    else:
        report_campaign_name = st.selectbox(
            "캠페인 선택",
            ["전체"] + [c["campaign_name"] for c in campaigns_for_report],
            key="f5_report_select"
        )

        if report_campaign_name == "전체":
            posts = get_paid_posts()
        else:
            rc = next(c for c in campaigns_for_report if c["campaign_name"] == report_campaign_name)
            posts = get_paid_posts(rc["id"])

        if not posts:
            st.info("등록된 포스팅이 없습니다.")
        else:
            posts_df = pd.DataFrame(posts)

            # 성과 요약 메트릭
            total_fee = posts_df["fee"].sum() or 0
            total_views = posts_df["views"].sum() or 0
            avg_cpv = posts_df["cpv"].mean() if posts_df["cpv"].notna().any() else None
            avg_er = posts_df["er_pct"].mean() if posts_df["er_pct"].notna().any() else None

            m_cols = st.columns(4)
            with m_cols[0]:
                st.metric("총 집행 비용", f"¥{int(total_fee):,}")
            with m_cols[1]:
                st.metric("총 조회수", f"{int(total_views):,}")
            with m_cols[2]:
                st.metric("평균 CPV", f"¥{avg_cpv:.2f}" if avg_cpv else "—")
            with m_cols[3]:
                st.metric("평균 ER", f"{avg_er:.2f}%" if avg_er else "—")

            # 캠페인별 성과 테이블
            st.markdown("#### 포스팅 상세")
            report_cols = ["kol_name", "platform", "url", "post_date", "content_type",
                           "fee", "views", "likes", "comments", "saves",
                           "cpv", "cpe", "er_pct"]
            report_cols = [c for c in report_cols if c in posts_df.columns]
            display_posts = posts_df[report_cols].rename(columns={
                "kol_name": "KOL명", "platform": "플랫폼", "url": "URL",
                "post_date": "포스팅일", "content_type": "소재유형",
                "fee": "비용(JPY)", "views": "조회수", "likes": "좋아요",
                "comments": "댓글", "saves": "저장",
                "cpv": "CPV(¥/회)", "cpe": "CPE(¥/건)", "er_pct": "ER%",
            })
            st.dataframe(display_posts, use_container_width=True, hide_index=True, height=300)

            # KOL별 CPV 비교 차트
            cpv_data = posts_df[posts_df["cpv"].notna()][["kol_name", "cpv"]].copy()
            if not cpv_data.empty:
                st.markdown("#### KOL별 CPV 비교")
                cpv_chart = cpv_data.rename(columns={"kol_name": "KOL명", "cpv": "CPV(¥)"})
                st.bar_chart(cpv_chart.set_index("KOL명"), y="CPV(¥)", use_container_width=True)

            # ──────────────────────────────────────────────
            #  A4: 포스팅 소재 분석
            # ──────────────────────────────────────────────
            type_data = pd.DataFrame()
            type_summary = pd.DataFrame()
            a4_recommendation = ""

            if "content_type" in posts_df.columns:
                type_data = posts_df[
                    posts_df["content_type"].str.strip().ne("") & posts_df["cpv"].notna()
                ]

            if not type_data.empty:
                st.markdown("---")
                st.markdown("#### 🎬 소재 유형별 분석 (A4)")

                type_summary = type_data.groupby("content_type").agg(
                    포스팅수=("id", "count"),
                    평균CPV=("cpv", "mean"),
                    평균CPE=("cpe", "mean"),
                    평균ER=("er_pct", "mean"),
                    평균조회수=("views", "mean"),
                    총조회수=("views", "sum"),
                    총비용=("fee", "sum"),
                ).round(2).reset_index().rename(columns={"content_type": "소재유형"})

                # 효율 점수 계산: ER/CPV (높을수록 좋음)
                type_summary["효율점수"] = type_summary.apply(
                    lambda r: round(r["평균ER"] / r["평균CPV"], 4)
                    if r["평균CPV"] and r["평균CPV"] > 0 else None, axis=1
                )
                type_summary = type_summary.sort_values("효율점수", ascending=False, na_position="last")

                st.dataframe(type_summary, use_container_width=True, hide_index=True)

                # 소재 유형별 CPV 비교 차트
                st.markdown("##### 소재 유형별 평균 CPV 비교")
                cpv_by_type = type_summary[["소재유형", "평균CPV"]].dropna().set_index("소재유형")
                if not cpv_by_type.empty:
                    st.bar_chart(cpv_by_type, y="평균CPV", use_container_width=True)

                # 소재 유형별 ER 비교 차트
                st.markdown("##### 소재 유형별 평균 ER 비교")
                er_by_type = type_summary[["소재유형", "평균ER"]].dropna().set_index("소재유형")
                if not er_by_type.empty:
                    st.bar_chart(er_by_type, y="평균ER", use_container_width=True)

                # 최고 효율 소재 + 추천
                best = type_summary.iloc[0] if not type_summary.empty else None
                worst = type_summary.iloc[-1] if len(type_summary) > 1 else None

                if best is not None:
                    st.markdown("##### 💡 소재 분석 인사이트")

                    insights = []
                    insights.append(
                        f"- **가장 효율적인 소재**: **{best['소재유형']}** "
                        f"(평균 CPV ¥{best['평균CPV']:.2f}, ER {best['평균ER']:.2f}%)"
                    )
                    if worst is not None and worst["소재유형"] != best["소재유형"]:
                        insights.append(
                            f"- **가장 비효율적인 소재**: **{worst['소재유형']}** "
                            f"(평균 CPV ¥{worst['평균CPV']:.2f}, ER {worst['평균ER']:.2f}%)"
                        )

                    # CPV 대비 ER이 높은 소재 = 가성비 최고
                    if best.get("효율점수"):
                        insights.append(
                            f"- **다음 캠페인 추천 소재**: **{best['소재유형']}** 형식을 "
                            f"우선적으로 활용하세요 (효율점수 {best['효율점수']:.4f})"
                        )

                    # 샘플 수 부족 경고
                    low_sample = type_summary[type_summary["포스팅수"] < 3]
                    if not low_sample.empty:
                        names = ", ".join(low_sample["소재유형"].tolist())
                        insights.append(
                            f"- ⚠️ **{names}**은(는) 샘플 3건 미만으로 통계 신뢰도가 낮습니다."
                        )

                    for line in insights:
                        st.markdown(line)
                    a4_recommendation = "\n".join(insights)

            # Excel 다운로드
            st.markdown("---")
            try:
                f5_excel_buf = io.BytesIO()
                with pd.ExcelWriter(f5_excel_buf, engine="openpyxl") as writer:
                    display_posts.to_excel(writer, sheet_name="포스팅 상세", index=False)
                    if not type_summary.empty:
                        type_summary.to_excel(writer, sheet_name="소재별 분석", index=False)
                    if a4_recommendation:
                        rec_df = pd.DataFrame({"인사이트": a4_recommendation.split("\n")})
                        rec_df.to_excel(writer, sheet_name="소재 인사이트", index=False)
                f5_excel_buf.seek(0)
                st.download_button(
                    "📥 성과 리포트 Excel 다운로드",
                    data=f5_excel_buf.getvalue(),
                    file_name=f"KOL_캠페인_리포트_{report_campaign_name}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
            except Exception as e:
                st.error(f"Excel 생성 오류: {e}")


# ──────────────────────────────────────────────────────────────
#  F6: 노출-쿼리 상관관계 분석
# ──────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("### 📈 노출-쿼리 상관관계 분석 (F6)")
st.caption("유가 포스팅의 노출수 추이와 브랜드 검색량 변화의 상관관계를 분석합니다.")

f6_col1, f6_col2 = st.columns([2, 2])
with f6_col1:
    f6_brand_kw = st.text_input(
        "Google Trends 검색 키워드",
        placeholder="예: ANESSA 日焼け止め",
        key="f6_brand_kw"
    )
with f6_col2:
    f6_campaigns_list = get_campaigns()
    f6_campaign_names = ["전체"] + [c["campaign_name"] for c in f6_campaigns_list]
    f6_sel_campaign = st.selectbox("캠페인 선택", f6_campaign_names, key="f6_campaign")

f6_col3, f6_col4 = st.columns([2, 2])
with f6_col3:
    f6_manual_query = st.text_area(
        "수동 검색량 입력 (큐텐/아마존JP 등, 선택사항)",
        placeholder="2024-01-01,120\n2024-01-02,135\n...",
        height=80, key="f6_manual"
    )
with f6_col4:
    f6_timeframe = st.selectbox(
        "분석 기간",
        ["최근 1개월", "최근 2개월", "최근 3개월"],
        index=1, key="f6_timeframe"
    )

if st.button("📊 상관관계 분석 실행", type="primary", use_container_width=True, key="f6_run"):
    # 1. 노출수 데이터 수집
    sel_cid = None
    if f6_sel_campaign != "전체":
        sel_c = next(c for c in f6_campaigns_list if c["campaign_name"] == f6_sel_campaign)
        sel_cid = sel_c["id"]

    daily_imp = get_daily_impressions(sel_cid)

    if not daily_imp:
        st.warning("포스팅 데이터가 없습니다. F5에서 유가 포스팅을 먼저 등록하세요.")
    else:
        imp_df = pd.DataFrame(daily_imp)
        imp_df["post_date"] = pd.to_datetime(imp_df["post_date"], errors="coerce")
        imp_df = imp_df.dropna(subset=["post_date"]).sort_values("post_date")

        # 기간 필터
        from datetime import timedelta
        months_map = {"최근 1개월": 30, "최근 2개월": 60, "최근 3개월": 90}
        days_back = months_map.get(f6_timeframe, 60)
        cutoff = pd.Timestamp.now() - timedelta(days=days_back)
        imp_df = imp_df[imp_df["post_date"] >= cutoff]

        # 2. Google Trends 데이터 수집
        trends_df = None
        if f6_brand_kw.strip():
            try:
                from pytrends.request import TrendReq
                pytrends = TrendReq(hl="ja-JP", tz=540)
                tf_map = {"최근 1개월": "today 1-m", "최근 2개월": "today 3-m", "최근 3개월": "today 3-m"}
                pytrends.build_payload(
                    [f6_brand_kw.strip()],
                    cat=0, timeframe=tf_map.get(f6_timeframe, "today 3-m"),
                    geo="JP"
                )
                trends_df = pytrends.interest_over_time()
                if not trends_df.empty and "isPartial" in trends_df.columns:
                    trends_df = trends_df.drop(columns=["isPartial"])
                if not trends_df.empty:
                    trends_df = trends_df.reset_index()
                    trends_df.columns = ["date", "search_volume"]
            except Exception as e:
                st.warning(f"Google Trends 수집 실패: {e}. 수동 입력 데이터를 사용합니다.")

        # 수동 검색량 파싱
        manual_df = None
        if f6_manual_query.strip():
            try:
                rows = []
                for line in f6_manual_query.strip().split("\n"):
                    parts = line.split(",")
                    if len(parts) >= 2:
                        rows.append({"date": parts[0].strip(), "search_volume": float(parts[1].strip())})
                if rows:
                    manual_df = pd.DataFrame(rows)
                    manual_df["date"] = pd.to_datetime(manual_df["date"], errors="coerce")
                    manual_df = manual_df.dropna(subset=["date"])
            except Exception:
                pass

        # 검색량 데이터 결합 (Google Trends 우선, 수동 보충)
        query_df = None
        if trends_df is not None and not trends_df.empty:
            query_df = trends_df.copy()
            query_df["date"] = pd.to_datetime(query_df["date"])
        if manual_df is not None and not manual_df.empty:
            if query_df is not None:
                query_df = pd.concat([query_df, manual_df]).groupby("date").sum().reset_index()
            else:
                query_df = manual_df

        # 3. 이중 축 차트
        if imp_df.empty:
            st.warning("분석 기간 내 노출 데이터가 없습니다.")
        else:
            # 노출 데이터 준비
            chart_imp = imp_df[["post_date", "daily_views"]].rename(
                columns={"post_date": "date", "daily_views": "노출수"}
            )
            chart_imp["date"] = pd.to_datetime(chart_imp["date"])

            if query_df is not None and not query_df.empty:
                query_df["date"] = pd.to_datetime(query_df["date"])
                merged = pd.merge(chart_imp, query_df, on="date", how="outer").sort_values("date")
                merged = merged.fillna(0)

                st.markdown("#### 노출수 vs 검색량 추이")

                # 정규화해서 동일 스케일로 표시
                max_imp = merged["노출수"].max() or 1
                max_query = merged["search_volume"].max() or 1
                merged["노출수(정규화)"] = merged["노출수"] / max_imp * 100
                merged["검색량(정규화)"] = merged["search_volume"] / max_query * 100

                chart_data = merged.set_index("date")[["노출수(정규화)", "검색량(정규화)"]]
                st.line_chart(chart_data, use_container_width=True)

                # 피어슨 상관계수
                valid_merged = merged[(merged["노출수"] > 0) & (merged["search_volume"] > 0)]
                if len(valid_merged) >= 5:
                    corr = valid_merged["노출수"].corr(valid_merged["search_volume"])

                    if abs(corr) >= 0.7:
                        strength = "강한"
                    elif abs(corr) >= 0.4:
                        strength = "중간"
                    else:
                        strength = "약한"
                    direction = "양의" if corr > 0 else "음의"

                    st.markdown(f"#### 📐 상관관계 분석")
                    st.metric("피어슨 상관계수", f"{corr:.3f}")
                    st.markdown(
                        f"**해석**: {strength} {direction} 상관관계 (r={corr:.3f}). "
                    )

                    # 시차 상관 분석 (1~7일 래그)
                    st.markdown("##### 시차별 상관계수 (노출 → 검색량)")
                    lag_results = []
                    for lag in range(0, 8):
                        shifted = merged.copy()
                        shifted["search_shifted"] = shifted["search_volume"].shift(-lag)
                        valid = shifted.dropna(subset=["노출수", "search_shifted"])
                        valid = valid[(valid["노출수"] > 0) & (valid["search_shifted"] > 0)]
                        if len(valid) >= 3:
                            lag_corr = valid["노출수"].corr(valid["search_shifted"])
                            lag_results.append({"시차(일)": lag, "상관계수": round(lag_corr, 3)})

                    if lag_results:
                        lag_df = pd.DataFrame(lag_results)
                        st.dataframe(lag_df, use_container_width=True, hide_index=True)
                        best_lag = max(lag_results, key=lambda x: abs(x["상관계수"]))
                        if best_lag["시차(일)"] > 0:
                            st.markdown(
                                f"- 노출 증가 후 약 **{best_lag['시차(일)']}일 뒤** "
                                f"검색량 변화가 가장 큰 패턴 (r={best_lag['상관계수']:.3f})"
                            )
                else:
                    st.info("상관 분석을 위한 데이터가 부족합니다 (최소 5일치 필요).")

                # 노출 스파이크 마킹
                posting_dates = imp_df[imp_df["post_count"] > 0]["post_date"].dt.strftime("%Y-%m-%d").tolist()
                if posting_dates:
                    st.markdown(f"##### 📌 유가 포스팅 집행일: {', '.join(posting_dates)}")

            else:
                # 검색량 없이 노출수만 표시
                st.markdown("#### 노출수 추이")
                st.line_chart(chart_imp.set_index("date"), y="노출수", use_container_width=True)
                st.info("검색량 데이터를 입력하면 상관관계 분석이 가능합니다.")


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
