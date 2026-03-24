"""
Menu 2: 콘텐츠 실시간 탐색 (F1 + F2)
"""

import streamlit as st
import pandas as pd
import re
import time
from pathlib import Path
from collections import Counter
from copy import deepcopy

from scraper import search_brand, extract_kol_candidates
from scorer import PLATFORM_WEIGHTS

# ──────────────────────────────────────────────────────────────
#  국가별 언어 매핑 + 브랜드명 변환
# ──────────────────────────────────────────────────────────────
COUNTRY_CONFIG = {
    "일본 🇯🇵": {
        "code": "JP", "lang": "ja",
        "brand_suffix": "",
        "note": "일본어 표기/발음으로 자동 변환하여 검색합니다.",
    },
    "한국 🇰🇷": {
        "code": "KR", "lang": "ko",
        "brand_suffix": "",
        "note": "한국어로 검색합니다.",
    },
    "미국 🇺🇸": {
        "code": "US", "lang": "en",
        "brand_suffix": "",
        "note": "영어로 검색합니다.",
    },
    "중국 🇨🇳": {
        "code": "CN", "lang": "zh",
        "brand_suffix": "",
        "note": "중국어로 검색합니다.",
    },
    "태국 🇹🇭": {
        "code": "TH", "lang": "th",
        "brand_suffix": "",
        "note": "태국어로 검색합니다.",
    },
    "베트남 🇻🇳": {
        "code": "VN", "lang": "vi",
        "brand_suffix": "",
        "note": "베트남어로 검색합니다.",
    },
    "글로벌 🌐": {
        "code": "GLOBAL", "lang": "en",
        "brand_suffix": "",
        "note": "글로벌 검색 (영어 기반)",
    },
}

# 일본어 브랜드 변환 사전 (자주 사용되는 K-뷰티 브랜드 등)
JP_BRAND_MAP = {
    "UNOVE":     ["アノブ", "UNOVE"],
    "ANESSA":    ["アネッサ", "ANESSA"],
    "BIORE":     ["ビオレ", "BIORE"],
    "SKIN AQUA": ["スキンアクア", "SKIN AQUA"],
    "INNISFREE": ["イニスフリー", "INNISFREE"],
    "LANEIGE":   ["ラネージュ", "LANEIGE"],
    "ETUDE":     ["エチュード", "ETUDE"],
    "MISSHA":    ["ミシャ", "MISSHA"],
    "CLIO":      ["クリオ", "CLIO"],
    "TIRTIR":    ["ティルティル", "TIRTIR"],
    "AMUSE":     ["アミューズ", "AMUSE"],
    "ROM&ND":    ["ロムアンド", "rom&nd"],
    "COSRX":     ["コスアールエックス", "COSRX"],
    "MEDICUBE":  ["メディキューブ", "MEDICUBE"],
    "VT":        ["ブイティー", "VT"],
    "TORRIDEN":  ["トリデン", "TORRIDEN"],
}


def _localize_brand(brand: str, country_key: str) -> list[str]:
    """브랜드명을 해당 국가 언어로 변환. 여러 변형을 반환."""
    brand_upper = brand.strip().upper()
    config = COUNTRY_CONFIG.get(country_key, {})

    # 일본 선택 시 JP 브랜드맵 적용
    if config.get("code") == "JP" and brand_upper in JP_BRAND_MAP:
        return JP_BRAND_MAP[brand_upper]

    # 기본: 원래 브랜드명 그대로
    return [brand.strip()]


def render_explore_page(IS_CLOUD):
    st.markdown("### 🔎 콘텐츠 실시간 탐색")
    st.caption("브랜드명을 입력하면 해당 국가의 플랫폼에서 관련 콘텐츠를 수집·분석합니다.")

    # ── 입력 영역 ──
    e_col1, e_col2, e_col3 = st.columns([2, 2, 1])
    with e_col1:
        own_brand = st.text_input("자사 브랜드명", placeholder="예: UNOVE", key="e_own")
    with e_col2:
        competitors = st.text_input("경쟁사 브랜드 (쉼표 구분)", placeholder="예: TIRTIR, COSRX", key="e_comp")
    with e_col3:
        country = st.selectbox("국가", list(COUNTRY_CONFIG.keys()), index=0, key="e_country")

    country_cfg = COUNTRY_CONFIG[country]
    st.caption(f"💡 {country_cfg['note']}")

    e_col4, e_col5 = st.columns([1, 1])
    with e_col4:
        platforms = st.multiselect(
            "검색 플랫폼", ["TikTok", "Instagram", "YouTube"],
            default=["TikTok", "YouTube"], key="e_plat"
        )
    with e_col5:
        days = st.slider("수집 기간 (일)", 7, 180, 90, key="e_days")

    if st.button("🔍 콘텐츠 탐색 시작", type="primary", use_container_width=True, key="e_run"):
        # 브랜드 목록 구성
        brands_raw = []
        if own_brand.strip():
            brands_raw.append(own_brand.strip())
        for comp in competitors.split(","):
            if comp.strip():
                brands_raw.append(comp.strip())

        if not brands_raw:
            st.warning("브랜드명을 입력하세요.")
            return
        if not platforms:
            st.warning("플랫폼을 선택하세요.")
            return

        # 각 브랜드를 국가 언어로 변환
        search_tasks = []
        for brand in brands_raw:
            localized = _localize_brand(brand, country)
            search_tasks.append({"original": brand, "queries": localized})

        progress = st.progress(0, text="탐색 준비 중...")
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

        all_posts = []
        total_tasks = len(search_tasks)
        for bi, task in enumerate(search_tasks):
            for qi, query in enumerate(task["queries"]):
                progress.progress(
                    (bi + qi / len(task["queries"])) / total_tasks,
                    text=f"'{query}' ({task['original']}) 검색 중..."
                )

                pw_page = None
                if pw_ctx:
                    try: pw_page = pw_ctx.new_page()
                    except: pw_page = None

                # max_results는 days 기반 (일당 약 1~2개 추정)
                max_r = max(days // 3, 10)
                posts = search_brand(query, platforms, max_r, pw_page)

                # 원본 브랜드명 태깅
                for p in posts:
                    p["brand"] = task["original"]
                    p["search_query"] = query
                all_posts.extend(posts)

                if pw_page:
                    try: pw_page.close()
                    except: pass

        if pw_ctx:
            try:
                pw_ctx.close()
                _pw.__exit__(None, None, None)
            except: pass

        progress.progress(1.0, text="✅ 탐색 완료!")
        st.session_state.explore_results = all_posts

    # ══════════════════════════════════════════════════════════
    #  결과 표시
    # ══════════════════════════════════════════════════════════
    if not st.session_state.get("explore_results"):
        return

    data = st.session_state.explore_results
    df = pd.DataFrame(data)

    if df.empty:
        st.info("검색 결과가 없습니다.")
        return

    # ── 필터 영역 ──
    st.markdown("#### 📋 브랜드별 콘텐츠 목록")

    filt_cols = st.columns([1, 1, 1, 1])
    with filt_cols[0]:
        brand_filter = st.multiselect(
            "브랜드", df["brand"].unique().tolist(),
            default=df["brand"].unique().tolist(), key="e_brand_f"
        )
    with filt_cols[1]:
        plat_filter = st.multiselect(
            "플랫폼", df["platform"].unique().tolist(),
            default=df["platform"].unique().tolist(), key="e_plat_f"
        )
    with filt_cols[2]:
        sort_by = st.selectbox(
            "정렬 기준", ["조회수", "좋아요", "댓글", "저장"],
            key="e_sort"
        )
    with filt_cols[3]:
        sort_order = st.selectbox("정렬", ["내림차순", "오름차순"], key="e_order")

    filtered = df.copy()
    if brand_filter:
        filtered = filtered[filtered["brand"].isin(brand_filter)]
    if plat_filter:
        filtered = filtered[filtered["platform"].isin(plat_filter)]

    sort_col_map = {"조회수": "views", "좋아요": "likes", "댓글": "comments", "저장": "saves"}
    sort_col = sort_col_map[sort_by]
    ascending = sort_order == "오름차순"
    filtered = filtered.sort_values(sort_col, ascending=ascending, na_position="last")

    # ── 테이블 구성 ──
    display = filtered[["brand", "kol_name", "platform", "views", "likes",
                         "comments", "saves", "url"]].copy()
    display.columns = ["브랜드", "KOL명", "플랫폼", "조회수", "좋아요", "댓글", "저장", "URL"]

    # 숫자 포맷팅 (쉼표)
    for col in ["조회수", "좋아요", "댓글", "저장"]:
        display[col] = display[col].apply(
            lambda x: f"{int(x):,}" if pd.notna(x) and x is not None else "N/A"
        )

    # URL을 클릭 가능한 링크로
    display["URL"] = display["URL"].apply(
        lambda u: f'<a href="{u}" target="_blank" class="link-btn">🔗 보기</a>' if u else ""
    )

    st.markdown(
        display.to_html(escape=False, index=False),
        unsafe_allow_html=True
    )
    st.caption(f"총 {len(filtered)}건")

    # ── 바이럴 Top 10 ──
    top10 = filtered.dropna(subset=["views"]).nlargest(10, "views")
    if not top10.empty:
        st.markdown("#### 🔥 바이럴 콘텐츠 Top 10")
        for idx, (_, row) in enumerate(top10.iterrows()):
            views_str = f"{int(row['views']):,}" if row.get('views') else "N/A"
            likes_str = f"{int(row['likes']):,}" if pd.notna(row.get('likes')) else "N/A"
            st.markdown(
                f"**{idx+1}.** {row.get('kol_name','—')} ({row['platform']}) — "
                f"조회수 {views_str} / 좋아요 {likes_str} | "
                f'<a href="{row["url"]}" target="_blank" class="link-btn">🔗</a>',
                unsafe_allow_html=True
            )

    # ── 플랫폼별 평균 ER ──
    st.markdown("#### 📊 플랫폼별 평균 참여율")
    er_data = []
    for plat in filtered["platform"].unique():
        pp = filtered[filtered["platform"] == plat]
        views_sum = pp["views"].dropna().sum()
        eng_sum = pp["likes"].dropna().sum() + pp["comments"].dropna().sum() + pp["saves"].dropna().sum()
        er = round(eng_sum / views_sum * 100, 2) if views_sum > 0 else None
        er_data.append({
            "플랫폼": plat, "총 조회수": f"{int(views_sum):,}",
            "총 참여": f"{int(eng_sum):,}", "평균 ER%": er, "콘텐츠 수": len(pp),
        })
    st.dataframe(pd.DataFrame(er_data), use_container_width=True, hide_index=True)

    # ── 해시태그 Top 20 ──
    all_tags = []
    for tags in filtered["hashtags"].dropna():
        if isinstance(tags, list):
            all_tags.extend(tags)
    if all_tags:
        st.markdown("#### #️⃣ 해시태그 Top 20")
        tag_counts = Counter(all_tags).most_common(20)
        tag_df = pd.DataFrame(tag_counts, columns=["해시태그", "출현 횟수"])
        st.dataframe(tag_df, use_container_width=True, hide_index=True)

    # ══════════════════════════════════════════════════════════
    #  F2: 인사이트 & 벤치마크 보정
    # ══════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("### 💡 인사이트 & 벤치마크 보정")

    brands_in_data = filtered["brand"].unique().tolist()
    own = brands_in_data[0] if brands_in_data else ""

    # 자사 vs 경쟁사 비교
    if len(brands_in_data) >= 2:
        st.markdown("#### 자사 vs 경쟁사 평균 ER 비교")
        brand_er = []
        for b in brands_in_data:
            bp = filtered[filtered["brand"] == b]
            bv = bp["views"].dropna().sum()
            be = bp["likes"].dropna().sum() + bp["comments"].dropna().sum() + bp["saves"].dropna().sum()
            ber = round(be / bv * 100, 2) if bv > 0 else None
            label = f"🏠 {b}" if b == own else b
            brand_er.append({"브랜드": label, "평균 ER%": ber, "콘텐츠 수": len(bp)})
        brand_er_df = pd.DataFrame(brand_er)
        st.dataframe(brand_er_df, use_container_width=True, hide_index=True)
        chart_data = brand_er_df.dropna(subset=["평균 ER%"])
        if not chart_data.empty:
            st.bar_chart(chart_data.set_index("브랜드"), y="평균 ER%", use_container_width=True)

    # 바이럴 패턴
    st.markdown("#### 🧩 바이럴 콘텐츠 패턴")
    if all_tags:
        st.markdown(f"- **주요 해시태그**: {', '.join(t for t, _ in tag_counts[:10])}")
    plat_dist = filtered["platform"].value_counts()
    st.markdown(f"- **플랫폼 분포**: {' / '.join(f'{p} {c}건' for p, c in plat_dist.items())}")
    viral_q = filtered["views"].dropna()
    if len(viral_q) > 5:
        threshold = viral_q.quantile(0.9)
        st.markdown(f"- **상위 10% 조회수 기준**: {int(threshold):,}회 이상")

    # 벤치마크 보정
    st.markdown("#### 📐 벤치마크 보정")
    benchmark = []
    for plat in filtered["platform"].unique():
        pp = filtered[filtered["platform"] == plat]
        v = pp["views"].dropna().sum()
        eng = pp["likes"].dropna().sum() + pp["comments"].dropna().sum() + pp["saves"].dropna().sum()
        mer = round(eng / v * 100, 2) if v > 0 else None
        benchmark.append({"플랫폼": plat, "시장 평균 ER%": mer, "샘플 수": len(pp)})
    bm_df = pd.DataFrame(benchmark)
    st.dataframe(bm_df, use_container_width=True, hide_index=True)

    if "benchmark_history" not in st.session_state:
        st.session_state.benchmark_history = []

    if st.button("📌 벤치마크 보정값 저장", key="f2_save_bm"):
        from datetime import datetime
        entry = {"date": datetime.now().strftime("%Y-%m-%d %H:%M"), "benchmarks": benchmark}
        st.session_state.benchmark_history.append(entry)
        st.success("✅ 벤치마크 보정값 저장됨")

    if st.session_state.get("benchmark_history"):
        with st.expander("📜 보정 이력"):
            for entry in reversed(st.session_state.benchmark_history):
                st.markdown(f"**{entry['date']}**")
                st.dataframe(pd.DataFrame(entry["benchmarks"]), use_container_width=True, hide_index=True)
