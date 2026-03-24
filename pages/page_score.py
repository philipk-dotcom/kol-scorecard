"""
Menu 1: KOL 스코어 시스템
Step 0: 평가 기준 설정
Step 1: KOL 정보 입력
Step 2: 자동 스크래핑
Step 3: 평가 결과
"""

import streamlit as st
import pandas as pd
import numpy as np
import re
import time
import io
import json
from pathlib import Path
from copy import deepcopy

from scraper import scrape_kol, detect_platform, extract_username
from scorer import (KOLMetrics, calculate_all_scores, kols_to_dataframe,
                    analyze_audience_quality, PLATFORM_WEIGHTS)
from export import generate_excel_scorecard
from db import save_scored_df

# ──────────────────────────────────────────────────────────────
#  기본 벤치마크 (복원용)
# ──────────────────────────────────────────────────────────────
DEFAULT_WEIGHTS = {
    "TikTok": [
        {"지표": "CPV (조회당 비용)",    "코드": "cpv",           "방향": "낮을수록 좋음", "가중치": 0.35},
        {"지표": "ER% (참여율)",         "코드": "er_pct",        "방향": "높을수록 좋음", "가중치": 0.35},
        {"지표": "저장률%",              "코드": "save_rate_pct", "방향": "높을수록 좋음", "가중치": 0.30},
    ],
    "Instagram": [
        {"지표": "CPE (참여당 비용)",    "코드": "cpe",              "방향": "낮을수록 좋음", "가중치": 0.60},
        {"지표": "댓글비율%",            "코드": "comment_ratio_pct","방향": "높을수록 좋음", "가중치": 0.40},
    ],
    "YouTube": [
        {"지표": "CPV (조회당 비용)",    "코드": "cpv", "방향": "낮을수록 좋음", "가중치": 1.00},
    ],
    "Twitter": [
        {"지표": "CPV (조회당 비용)",    "코드": "cpv",    "방향": "낮을수록 좋음", "가중치": 0.50},
        {"지표": "ER% (참여율)",         "코드": "er_pct", "방향": "높을수록 좋음", "가중치": 0.30},
        {"지표": "CPE (참여당 비용)",    "코드": "cpe",    "방향": "낮을수록 좋음", "가중치": 0.20},
    ],
    "Lipscosme": [
        {"지표": "CPE (참여당 비용)",    "코드": "cpe",            "방향": "낮을수록 좋음", "가중치": 0.50},
        {"지표": "저장비율%",            "코드": "save_ratio_pct", "방향": "높을수록 좋음", "가중치": 0.50},
    ],
}

DEFAULT_GRADE_THRESHOLDS = {
    "★★★★★ 최우선 채택": 8.5,
    "★★★★ 채택 권고":   7.0,
    "★★★ 조건부 채택":   5.5,
    "★★ 보류":           3.0,
    "★ 비권고":           0.0,
}


def _load_saved_weights():
    """저장된 가중치 불러오기"""
    if "custom_weights" in st.session_state:
        return st.session_state.custom_weights
    return None


def _apply_weights_to_scorer(weights_dict):
    """수정된 가중치를 scorer 모듈에 반영"""
    import scorer
    new_weights = {}
    for plat, rows in weights_dict.items():
        new_weights[plat] = []
        for row in rows:
            higher = row["방향"] == "높을수록 좋음"
            new_weights[plat].append((row["코드"], higher, row["가중치"]))
    scorer.PLATFORM_WEIGHTS = new_weights


def render_score_page(IS_CLOUD, num_posts, pinned_global):
    # ──────────────────────────────────────────────────────────
    #  세션 상태 초기화
    # ──────────────────────────────────────────────────────────
    if "kol_rows" not in st.session_state:
        st.session_state.kol_rows = pd.DataFrame({
            "KOL명": [""], "URL": [""], "플랫폼": ["자동감지"],
            "비용(JPY)": [0], "핀게시물ID": [""],
        })
    if "scraped_results" not in st.session_state:
        st.session_state.scraped_results = None
    if "scored_df" not in st.session_state:
        st.session_state.scored_df = None

    # ══════════════════════════════════════════════════════════
    #  STEP 0: 평가 기준 설정
    # ══════════════════════════════════════════════════════════
    st.markdown('<span class="step-badge">STEP 0</span> **평가 기준 설정**', unsafe_allow_html=True)

    # 저장된 프리셋 관리
    if "weight_presets" not in st.session_state:
        st.session_state.weight_presets = {"기본값": deepcopy(DEFAULT_WEIGHTS)}
    if "current_preset" not in st.session_state:
        st.session_state.current_preset = "기본값"

    # 드롭다운: 우측 정렬
    s0_col1, s0_col2, s0_col3 = st.columns([3, 1, 1])
    with s0_col1:
        st.caption("플랫폼별 가중치와 등급 기준을 확인·수정할 수 있습니다.")
    with s0_col2:
        preset_names = list(st.session_state.weight_presets.keys())
        selected_preset = st.selectbox(
            "프리셋 선택", preset_names,
            index=preset_names.index(st.session_state.current_preset),
            key="s0_preset", label_visibility="collapsed"
        )
    with s0_col3:
        save_name = st.text_input("프리셋명", value="", key="s0_save_name",
                                  placeholder="새 이름", label_visibility="collapsed")

    # 현재 가중치 로드
    if selected_preset != st.session_state.current_preset:
        st.session_state.current_preset = selected_preset
    current_weights = deepcopy(st.session_state.weight_presets.get(
        st.session_state.current_preset, DEFAULT_WEIGHTS
    ))

    with st.expander("📊 플랫폼별 가중치 설정", expanded=False):
        for plat, rows in current_weights.items():
            st.markdown(f"**{plat}**")
            plat_df = pd.DataFrame(rows)
            edited = st.data_editor(
                plat_df,
                use_container_width=True,
                hide_index=True,
                key=f"s0_weights_{plat}",
                column_config={
                    "지표": st.column_config.TextColumn("지표", disabled=True),
                    "코드": st.column_config.TextColumn("코드", disabled=True),
                    "방향": st.column_config.SelectboxColumn(
                        "방향", options=["높을수록 좋음", "낮을수록 좋음"], width="medium"
                    ),
                    "가중치": st.column_config.NumberColumn(
                        "가중치", min_value=0.0, max_value=1.0, step=0.05, format="%.2f"
                    ),
                },
            )
            current_weights[plat] = edited.to_dict("records")

    with st.expander("🎖️ 등급 기준", expanded=False):
        if "grade_thresholds" not in st.session_state:
            st.session_state.grade_thresholds = deepcopy(DEFAULT_GRADE_THRESHOLDS)
        grade_df = pd.DataFrame([
            {"등급": k, "최소 점수": v} for k, v in st.session_state.grade_thresholds.items()
        ])
        edited_grades = st.data_editor(
            grade_df, use_container_width=True, hide_index=True, key="s0_grades",
            column_config={
                "등급": st.column_config.TextColumn("등급", disabled=True),
                "최소 점수": st.column_config.NumberColumn("최소 점수", min_value=0.0, max_value=10.0, step=0.5),
            },
        )
        st.session_state.grade_thresholds = {
            row["등급"]: row["최소 점수"] for _, row in edited_grades.iterrows()
        }

    # 저장 / 복원 버튼
    btn_cols = st.columns(3)
    with btn_cols[0]:
        if st.button("💾 저장", use_container_width=True, key="s0_save"):
            name = save_name.strip() if save_name.strip() else st.session_state.current_preset
            st.session_state.weight_presets[name] = deepcopy(current_weights)
            st.session_state.current_preset = name
            st.session_state.custom_weights = current_weights
            _apply_weights_to_scorer(current_weights)
            st.success(f"✅ '{name}' 저장됨")
    with btn_cols[1]:
        if st.button("🔄 기본값 복원", use_container_width=True, key="s0_reset"):
            st.session_state.weight_presets["기본값"] = deepcopy(DEFAULT_WEIGHTS)
            st.session_state.current_preset = "기본값"
            st.session_state.grade_thresholds = deepcopy(DEFAULT_GRADE_THRESHOLDS)
            import scorer
            scorer.PLATFORM_WEIGHTS = {
                "TikTok":    [("cpv", False, 0.35), ("er_pct", True, 0.35), ("save_rate_pct", True, 0.30)],
                "Instagram": [("cpe", False, 0.60), ("comment_ratio_pct", True, 0.40)],
                "YouTube":   [("cpv", False, 1.00)],
                "Twitter":   [("cpv", False, 0.50), ("er_pct", True, 0.30), ("cpe", False, 0.20)],
                "Lipscosme": [("cpe", False, 0.50), ("save_ratio_pct", True, 0.50)],
            }
            st.success("✅ 기본값으로 복원됨")
    with btn_cols[2]:
        if st.button("▶ 적용 (점수에 반영)", use_container_width=True, key="s0_apply", type="primary"):
            _apply_weights_to_scorer(current_weights)
            st.session_state.custom_weights = current_weights
            st.success("✅ 가중치가 적용되었습니다. 점수 계산 시 반영됩니다.")

    # 저장된 가중치가 있으면 자동 적용
    if st.session_state.get("custom_weights"):
        _apply_weights_to_scorer(st.session_state.custom_weights)

    # ══════════════════════════════════════════════════════════
    #  STEP 1: KOL 정보 입력
    # ══════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown('<span class="step-badge">STEP 1</span> **KOL 정보 입력**', unsafe_allow_html=True)

    tab_manual, tab_csv = st.tabs(["✏️ 직접 입력", "📂 CSV/Excel 업로드"])

    with tab_manual:
        st.caption("URL을 입력하면 플랫폼이 자동 감지됩니다.")

        col_add, col_clear, col_sample = st.columns([1, 1, 2])
        with col_add:
            if st.button("➕ 행 추가", use_container_width=True, key="s1_add"):
                new_row = pd.DataFrame({
                    "KOL명": [""], "URL": [""], "플랫폼": ["자동감지"],
                    "비용(JPY)": [0], "핀게시물ID": [""]
                })
                st.session_state.kol_rows = pd.concat(
                    [st.session_state.kol_rows, new_row], ignore_index=True
                )
        with col_clear:
            if st.button("🗑 초기화", use_container_width=True, key="s1_clear"):
                st.session_state.kol_rows = pd.DataFrame({
                    "KOL명": [""], "URL": [""], "플랫폼": ["자동감지"],
                    "비용(JPY)": [0], "핀게시물ID": [""]
                })
                st.session_state.scraped_results = None
                st.session_state.scored_df = None
        with col_sample:
            if st.button("📋 샘플 데이터", use_container_width=True, key="s1_sample"):
                st.session_state.kol_rows = pd.DataFrame({
                    "KOL명": ["가연がよん", "もみー", "츠지짱", "𝙧𝙪𝙣.", "苺鈴"],
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

        edited = st.data_editor(
            st.session_state.kol_rows,
            use_container_width=True, num_rows="dynamic", hide_index=True,
            column_config={
                "KOL명": st.column_config.TextColumn("KOL명", width="medium"),
                "URL": st.column_config.TextColumn("URL (필수)", width="large"),
                "플랫폼": st.column_config.SelectboxColumn(
                    "플랫폼", options=["자동감지", "TikTok", "Instagram", "YouTube", "Twitter", "Lipscosme"],
                ),
                "비용(JPY)": st.column_config.NumberColumn("캐스팅 비용(JPY)", min_value=0, step=1000, format="¥%d"),
                "핀게시물ID": st.column_config.TextColumn("핀 게시물 ID", width="medium"),
            },
            key="kol_editor",
        )
        st.session_state.kol_rows = edited

        # 미리보기
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
                    "KOL명": row.get("KOL명", ""), "감지된 플랫폼": plat,
                    "유저명": uname,
                    "비용": f"¥{int(row.get('비용(JPY)') or 0):,}" if pd.notna(row.get('비용(JPY)')) else "¥0",
                })
            if preview_rows:
                st.markdown("**📍 입력 미리보기**")
                st.dataframe(pd.DataFrame(preview_rows), use_container_width=True, hide_index=True)

    with tab_csv:
        st.caption("CSV 또는 Excel 파일을 업로드하세요. 필수 열: **URL**")
        uploaded = st.file_uploader("파일 선택", type=["csv", "xlsx", "xls"], label_visibility="collapsed")
        if uploaded:
            try:
                df_up = pd.read_csv(uploaded) if uploaded.name.endswith(".csv") else pd.read_excel(uploaded)
                col_map = {}
                for c in df_up.columns:
                    cl = str(c).strip().lower()
                    if "url" in cl: col_map[c] = "URL"
                    elif any(x in cl for x in ["kol", "name", "이름", "명"]): col_map[c] = "KOL명"
                    elif any(x in cl for x in ["비용", "fee", "cost"]): col_map[c] = "비용(JPY)"
                    elif any(x in cl for x in ["핀", "pin"]): col_map[c] = "핀게시물ID"
                df_up = df_up.rename(columns=col_map)
                if "URL" not in df_up.columns:
                    st.error("'URL' 열이 없습니다.")
                else:
                    for col in ["KOL명", "비용(JPY)", "핀게시물ID"]:
                        if col not in df_up.columns:
                            df_up[col] = "" if col != "비용(JPY)" else 0
                    df_up["플랫폼"] = "자동감지"
                    st.session_state.kol_rows = df_up[["KOL명", "URL", "플랫폼", "비용(JPY)", "핀게시물ID"]]
                    st.success(f"✅ {len(df_up)}명 로드 완료")
            except Exception as e:
                st.error(f"파일 오류: {e}")

    # ══════════════════════════════════════════════════════════
    #  STEP 2: 자동 스크래핑
    # ══════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown('<span class="step-badge">STEP 2</span> **자동 스크래핑**', unsafe_allow_html=True)

    df_input = st.session_state.kol_rows.copy()
    valid_rows = df_input[df_input["URL"].str.strip().str.len() > 5].copy()

    if valid_rows.empty:
        st.info("STEP 1에서 URL을 먼저 입력해주세요.")
    else:
        col_scrape, col_info = st.columns([1, 3])
        with col_scrape:
            run_scrape = st.button("🚀 스크래핑 시작", use_container_width=True, type="primary", key="s2_run")
        with col_info:
            st.markdown(f"**{len(valid_rows)}명** 스크래핑 예정")

        if run_scrape:
            results = []
            progress = st.progress(0, text="스크래핑 준비 중...")
            status_container = st.container()
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

            for i, (_, row) in enumerate(valid_rows.iterrows()):
                url = str(row.get("URL", "")).strip()
                plat_override = str(row.get("플랫폼", "")).strip()
                plat = detect_platform(url) if plat_override in ("자동감지", "None", "nan", "") else plat_override

                name_raw = str(row.get("KOL명", "")).strip()
                if not name_raw or name_raw in ("None", "nan"):
                    name_raw = extract_username(url, plat)
                name = name_raw or url

                fee_raw = row.get("비용(JPY)", 0)
                fee = float(fee_raw) if fee_raw and str(fee_raw) not in ("", "nan", "None") else None

                pin_raw = str(row.get("핀게시물ID", "")).strip()
                row_pins = [p.strip() for p in re.split(r"[,\n]", pin_raw) if p.strip()]
                all_pins = list(set(pinned_global + row_pins))

                progress.progress((i + 0.5) / len(valid_rows), text=f"[{i+1}/{len(valid_rows)}] {name} ({plat})...")

                pw_page = None
                if pw_ctx:
                    try: pw_page = pw_ctx.new_page()
                    except: pw_page = None

                raw = scrape_kol(url=url, num_posts=num_posts, pinned_ids=all_pins, playwright_page=pw_page)

                if pw_page:
                    try: pw_page.close()
                    except: pass

                results.append({"name": name, "platform": plat, "url": url, "fee": fee, **raw})

                with status_container:
                    if raw["success"]:
                        views_disp = f'{raw["avg_views"]:,}' if raw["avg_views"] is not None else "N/A"
                        st.markdown(f'<span class="scrape-ok">✅ **{name}** ({plat}) — 조회수 {views_disp}</span>', unsafe_allow_html=True)
                    else:
                        st.markdown(f'<span class="scrape-fail">❌ **{name}** ({plat}) — {raw["error"]}</span>', unsafe_allow_html=True)
                time.sleep(0.5)

            if pw_ctx:
                try:
                    pw_ctx.close()
                    _pw.__exit__(None, None, None)
                except: pass

            progress.progress(1.0, text="✅ 스크래핑 완료!")
            st.session_state.scraped_results = results
            st.session_state.scored_df = None

    # ══════════════════════════════════════════════════════════
    #  STEP 3: 평가 결과
    # ══════════════════════════════════════════════════════════
    if st.session_state.scraped_results:
        st.markdown("---")
        st.markdown('<span class="step-badge">STEP 3</span> **평가 결과**', unsafe_allow_html=True)

        # 지표 보정 테이블
        with st.expander("📝 지표 수동 보정 (클릭하여 펼치기)", expanded=False):
            metric_rows = []
            for r in st.session_state.scraped_results:
                metric_rows.append({
                    "KOL명": r["name"], "플랫폼": r["platform"], "비용(JPY)": r["fee"],
                    "평균 조회수": r.get("avg_views"), "평균 좋아요": r.get("avg_likes"),
                    "평균 댓글": r.get("avg_comments"), "평균 저장": r.get("avg_saves"),
                    "평균 공유": r.get("avg_shares"), "게시물 수": r.get("post_count", 0),
                    "URL": r["url"], "오류": r.get("error", ""),
                })
            metric_df = pd.DataFrame(metric_rows)
            edited_metrics = st.data_editor(
                metric_df.drop(columns=["URL", "오류"]),
                use_container_width=True, hide_index=True,
                column_config={
                    "비용(JPY)": st.column_config.NumberColumn("비용(JPY)", format="¥%d"),
                    "평균 조회수": st.column_config.NumberColumn("평균 조회수", format="%d"),
                    "평균 좋아요": st.column_config.NumberColumn("평균 좋아요", format="%d"),
                    "평균 댓글": st.column_config.NumberColumn("평균 댓글", format="%d"),
                    "평균 저장": st.column_config.NumberColumn("평균 저장", format="%d"),
                    "평균 공유": st.column_config.NumberColumn("평균 공유", format="%d"),
                },
                key="metric_editor"
            )

        # 자동 점수 계산 (보정 후)
        if st.session_state.scraped_results and st.session_state.scored_df is None:
            # 자동 계산
            pass

        calc_btn = st.button("🏆 점수 계산", use_container_width=True, type="primary", key="s3_calc")

        if calc_btn:
            if "edited_metrics" not in dir() or edited_metrics is None:
                # fallback: scraped_results에서 직접 생성
                metric_rows2 = []
                for r in st.session_state.scraped_results:
                    metric_rows2.append({
                        "KOL명": r["name"], "플랫폼": r["platform"], "비용(JPY)": r["fee"],
                        "평균 조회수": r.get("avg_views"), "평균 좋아요": r.get("avg_likes"),
                        "평균 댓글": r.get("avg_comments"), "평균 저장": r.get("avg_saves"),
                        "평균 공유": r.get("avg_shares"), "게시물 수": r.get("post_count", 0),
                        "URL": r["url"],
                    })
                edited_metrics = pd.DataFrame(metric_rows2)
                metric_df = edited_metrics

            kol_list = []
            for idx, row in edited_metrics.iterrows():
                def _safe(val):
                    if val is None: return None
                    try:
                        f = float(val)
                        return None if np.isnan(f) else f
                    except: return None

                kol = KOLMetrics(
                    name=str(row["KOL명"]), platform=str(row["플랫폼"]),
                    url=metric_df.iloc[idx]["URL"] if "URL" in metric_df.columns else "",
                    fee=_safe(row.get("비용(JPY)")),
                    avg_views=_safe(row.get("평균 조회수")),
                    avg_likes=_safe(row.get("평균 좋아요")),
                    avg_comments=_safe(row.get("평균 댓글")),
                    avg_saves=_safe(row.get("평균 저장")),
                    avg_shares=_safe(row.get("평균 공유")),
                    post_count=int(row.get("게시물 수", 0) or 0),
                )
                kol_list.append(kol)

            scored = calculate_all_scores(kol_list)
            result_df = kols_to_dataframe(scored)
            st.session_state.scored_df = result_df
            st.session_state.quality_warnings = analyze_audience_quality(scored)
            saved_count = save_scored_df(result_df)
            st.success(f"✅ 점수 계산 완료! ({saved_count}명 DB 저장됨)")

    # ── 결과 표시 ──
    if st.session_state.scored_df is not None:
        df_result = st.session_state.scored_df
        quality_warnings = st.session_state.get("quality_warnings", {})

        # 요약 카드
        st.markdown("### 🏆 선별 결과")
        platforms_present = df_result["플랫폼"].dropna().unique().tolist()
        ordered = [p for p in ["TikTok","Instagram","YouTube","Twitter","Lipscosme"] if p in platforms_present]
        plat_icons = {"TikTok":"🎵","Instagram":"📸","YouTube":"▶️","Twitter":"🐦","Lipscosme":"💄"}

        if ordered:
            cols = st.columns(min(len(ordered), 5))
            for ci, plat in enumerate(ordered):
                with cols[ci % len(cols)]:
                    plat_df = (df_result[df_result["플랫폼"]==plat]
                               .dropna(subset=["종합점수"])
                               .sort_values("종합점수", ascending=False))
                    if plat_df.empty: continue
                    top = plat_df.iloc[0]
                    st.markdown(f"""
                    <div style="background:white;border-radius:10px;padding:12px;
                                box-shadow:0 2px 8px rgba(0,0,0,0.1);text-align:center;
                                border-top:4px solid #2e75b6;margin-bottom:8px;animation:fadeIn 0.4s ease;">
                      <div style="font-size:1.3rem;">{plat_icons.get(plat,'📊')}</div>
                      <div style="font-size:0.72rem;color:#666;">{plat}</div>
                      <div style="font-size:0.95rem;font-weight:700;color:#1a3a5c;">{top.get('KOL명','—')}</div>
                      <div style="font-size:1.4rem;font-weight:800;color:#2e75b6;">{top.get('종합점수',0):.1f}점</div>
                      <div style="font-size:0.85rem;">{top.get('등급','—')}</div>
                    </div>
                    """, unsafe_allow_html=True)

        # 품질 경고 추가
        display_df = df_result.copy()
        display_df["품질"] = display_df["KOL명"].apply(
            lambda n: f"⚠️ {len(quality_warnings[n])}건" if n in quality_warnings else "✅"
        )

        st.markdown("---")
        st.markdown("### 📊 전체 스코어카드")

        grade_filter = st.multiselect(
            "등급 필터", ["★★★★★","★★★★","★★★","★★","★"],
            default=["★★★★★","★★★★","★★★"], label_visibility="collapsed"
        )
        filtered = display_df[display_df["등급"].isin(grade_filter)] if grade_filter else display_df

        show_cols = ["KOL명","플랫폼","품질","비용(JPY)","평균 조회수","평균 좋아요",
                     "평균 댓글","평균 저장","CPV(¥/회)","ER%","저장률%","CPE(¥/건)",
                     "종합점수","등급","채택권고"]
        show_cols = [c for c in show_cols if c in filtered.columns]

        def _color_grade(val):
            return {"★★★★★":"background-color:#d5ead0;font-weight:800",
                    "★★★★":"background-color:#d5e8f0;font-weight:700",
                    "★★★":"background-color:#fffdd0","★★":"background-color:#ffe5cc",
                    "★":"background-color:#ffe0e0"}.get(str(val), "")

        styled = (filtered[show_cols].sort_values("종합점수", ascending=False, na_position="last")
                  .style.applymap(_color_grade, subset=["등급"])
                  .format({
                      "비용(JPY)": lambda x: f"¥{int(x):,}" if pd.notna(x) else "—",
                      "평균 조회수": lambda x: f"{int(x):,}" if pd.notna(x) else "—",
                      "평균 좋아요": lambda x: f"{int(x):,}" if pd.notna(x) else "—",
                      "평균 댓글": lambda x: f"{int(x):,}" if pd.notna(x) else "—",
                      "평균 저장": lambda x: f"{int(x):,}" if pd.notna(x) else "—",
                      "CPV(¥/회)": lambda x: f"¥{x:.2f}" if pd.notna(x) else "—",
                      "ER%": lambda x: f"{x:.2f}%" if pd.notna(x) else "—",
                      "저장률%": lambda x: f"{x:.2f}%" if pd.notna(x) else "—",
                      "CPE(¥/건)": lambda x: f"¥{x:.0f}" if pd.notna(x) else "—",
                      "종합점수": lambda x: f"{x:.1f}" if pd.notna(x) else "—",
                  }, na_rep="—"))
        st.dataframe(styled, use_container_width=True, height=400)

        # 품질 경고 상세
        if quality_warnings:
            with st.expander(f"🔍 오디언스 품질 경고 ({len(quality_warnings)}명)"):
                for kol_name, flags in quality_warnings.items():
                    st.markdown(f"**{kol_name}**")
                    for flag in flags:
                        st.markdown(f"- {flag}")

        # Excel 다운로드
        st.markdown("---")
        try:
            excel_bytes = generate_excel_scorecard(df_result)
            st.download_button(
                "📊 Excel 스코어카드 다운로드", data=excel_bytes,
                file_name="KOL_스코어카드.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True, type="primary"
            )
        except Exception as e:
            st.error(f"Excel 생성 오류: {e}")
