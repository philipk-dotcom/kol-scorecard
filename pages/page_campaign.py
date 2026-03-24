"""
캠페인 관리 페이지
F3: KOL 자동 발굴, A3: 예산 시뮬레이터,
F5+A4: 유가 포스팅 분석+소재 분석, F6: 노출-쿼리 상관분석
"""

import streamlit as st
import pandas as pd
import numpy as np
import re
import time
import io
from pathlib import Path
from datetime import timedelta

from scraper import (scrape_kol, detect_platform, extract_username,
                     search_brand, extract_kol_candidates)
from scorer import (KOLMetrics, calculate_all_scores, kols_to_dataframe,
                    analyze_audience_quality)
from db import (
    save_scored_df, get_all_kols,
    create_campaign, get_campaigns, add_paid_post, update_paid_post_metrics,
    get_paid_posts, get_campaign_summary, delete_campaign, get_daily_impressions,
)

CAMPAIGN_STATUSES = ["미접촉", "컨택 중", "진행 중", "완료", "보류"]
CONTENT_TYPES = ["", "언박싱", "데일리 루틴", "비포애프터", "튜토리얼", "리뷰", "이벤트·챌린지", "기타"]


def render_campaign_page(IS_CLOUD, num_posts):
    tool_tabs = st.tabs([
        "🔍 KOL 발굴",
        "💰 예산 시뮬레이터",
        "📈 유가 포스팅 분석",
        "📊 노출-쿼리 분석",
    ])

    # ══════════════════════════════════════════════════════════
    #  F3: KOL 자동 발굴
    # ══════════════════════════════════════════════════════════
    with tool_tabs[0]:
        st.markdown("### 🔍 KOL 자동 발굴")
        st.caption("키워드/해시태그로 KOL 후보를 탐색하고, 자동 스코어링 후 컨택 리스트를 생성합니다.")

        f3c1, f3c2 = st.columns([2, 1])
        with f3c1:
            f3_kw = st.text_input("검색 키워드/해시태그", placeholder="예: #선크림, 日焼け止め", key="f3_kw")
        with f3c2:
            f3_topn = st.number_input("발굴 인원 수", 5, 100, 30, key="f3_topn")

        f3c3, f3c4 = st.columns(2)
        with f3c3:
            f3_plat = st.multiselect("플랫폼", ["TikTok", "Instagram", "YouTube"], default=["TikTok"], key="f3_plat")
        with f3c4:
            f3_fee = st.number_input("예상 단가(JPY)", 0, step=10000, key="f3_fee")

        if st.button("🚀 KOL 발굴 시작", type="primary", use_container_width=True, key="f3_run"):
            keywords = [k.strip() for k in re.split(r"[,、]", f3_kw) if k.strip()]
            if not keywords or not f3_plat:
                st.warning("키워드와 플랫폼을 입력하세요.")
            else:
                prog = st.progress(0, text="후보 탐색 중...")
                browser_dir = Path.home() / ".kol_tool_session"
                pw_ctx, _pw = None, None
                if not IS_CLOUD:
                    try:
                        from playwright.sync_api import sync_playwright
                        _pw = sync_playwright().__enter__()
                        if browser_dir.exists():
                            pw_ctx = _pw.chromium.launch_persistent_context(str(browser_dir), headless=True)
                    except Exception:
                        pass

                all_sp = []
                for ki, kw in enumerate(keywords):
                    prog.progress((ki + 0.3) / (len(keywords) + 1), text=f"'{kw}' 검색 중...")
                    pw_page = None
                    if pw_ctx:
                        try:
                            pw_page = pw_ctx.new_page()
                        except Exception:
                            pw_page = None
                    posts = search_brand(kw, f3_plat, f3_topn * 2, pw_page)
                    all_sp.extend(posts)
                    if pw_page:
                        try:
                            pw_page.close()
                        except Exception:
                            pass

                candidates = extract_kol_candidates(all_sp)[:f3_topn]
                prog.progress(0.6, text=f"{len(candidates)}명 후보. 스코어링 중...")

                kol_list = []
                for ci, cand in enumerate(candidates):
                    prog.progress(
                        0.6 + 0.4 * (ci + 0.5) / max(len(candidates), 1),
                        text=f"[{ci+1}/{len(candidates)}] {cand['kol_name']}..."
                    )
                    pw_page = None
                    if pw_ctx:
                        try:
                            pw_page = pw_ctx.new_page()
                        except Exception:
                            pw_page = None
                    raw = scrape_kol(url=cand["profile_url"], num_posts=12,
                                     pinned_ids=[], playwright_page=pw_page)
                    if pw_page:
                        try:
                            pw_page.close()
                        except Exception:
                            pass
                    fee = float(f3_fee) if f3_fee else None
                    kol_list.append(KOLMetrics(
                        name=cand["kol_name"], platform=cand["platform"],
                        url=cand["profile_url"], fee=fee,
                        avg_views=raw.get("avg_views"), avg_likes=raw.get("avg_likes"),
                        avg_comments=raw.get("avg_comments"), avg_saves=raw.get("avg_saves"),
                        avg_shares=raw.get("avg_shares"), post_count=raw.get("post_count", 0),
                    ))
                    time.sleep(0.3)

                if pw_ctx:
                    try:
                        pw_ctx.close()
                        _pw.__exit__(None, None, None)
                    except Exception:
                        pass

                if kol_list:
                    scored = calculate_all_scores(kol_list)
                    contact_df = kols_to_dataframe(scored)
                    save_scored_df(contact_df)
                    st.session_state.f3_contact_list = contact_df
                prog.progress(1.0, text="✅ 완료!")

        if st.session_state.get("f3_contact_list") is not None:
            cdf = st.session_state.f3_contact_list
            st.markdown(f"#### 📋 컨택 리스트 ({len(cdf)}명)")
            cols = ["KOL명", "플랫폼", "평균 조회수", "평균 좋아요", "ER%",
                    "CPV(¥/회)", "종합점수", "등급", "채택권고", "URL"]
            cols = [c for c in cols if c in cdf.columns]
            st.dataframe(
                cdf[cols].sort_values("종합점수", ascending=False, na_position="last"),
                use_container_width=True, hide_index=True, height=400,
            )
            try:
                buf = io.BytesIO()
                with pd.ExcelWriter(buf, engine="openpyxl") as w:
                    cdf[cols].to_excel(w, sheet_name="컨택 리스트", index=False)
                buf.seek(0)
                st.download_button(
                    "📥 컨택 리스트 Excel", data=buf.getvalue(),
                    file_name="KOL_컨택리스트.xlsx", use_container_width=True, key="f3_dl",
                )
            except Exception as e:
                st.error(f"Excel 오류: {e}")

    # ══════════════════════════════════════════════════════════
    #  A3: 예산 시뮬레이터
    # ══════════════════════════════════════════════════════════
    with tool_tabs[1]:
        st.markdown("### 💰 캠페인 예산 시뮬레이터")
        st.caption("예산을 입력하면 KOL DB 기반으로 최적 KOL 조합을 추천합니다.")

        a3_raw = get_all_kols()
        a3_eligible = [
            k for k in a3_raw
            if k.get("latest_fee") and k.get("latest_views")
            and k["latest_fee"] > 0 and k["latest_views"] > 0
        ]

        if not a3_eligible:
            st.info("DB에 비용+조회수 데이터가 있는 KOL이 없습니다. 스코어카드를 먼저 실행하세요.")
        else:
            ac1, ac2 = st.columns(2)
            with ac1:
                budget = st.number_input("총 예산 (JPY)", 10000, step=50000, value=500000, key="a3_budget")
            with ac2:
                goal = st.selectbox("최적화 목표",
                                    ["총 노출 최대화", "총 인게이지먼트 최대화", "CPV 최소화"],
                                    key="a3_goal")

            if st.button("🧮 최적 조합 계산", type="primary", use_container_width=True, key="a3_run"):
                SCALE = 1000
                capacity = int(budget) // SCALE
                items = []
                for k in a3_eligible:
                    fee = max(int(k["latest_fee"]) // SCALE, 1)
                    views = k.get("latest_views") or 0
                    er = k.get("latest_er") or 0
                    eng = views * er / 100
                    if goal == "총 노출 최대화":
                        val = int(views)
                    elif goal == "총 인게이지먼트 최대화":
                        val = int(eng)
                    else:
                        val = int(views / fee) if fee > 0 else 0
                    items.append({"data": k, "fee_scaled": fee, "value": val})

                dp = [0] * (capacity + 1)
                keep = [[False] * (capacity + 1) for _ in range(len(items))]
                for i in range(len(items)):
                    w, v = items[i]["fee_scaled"], items[i]["value"]
                    for c in range(capacity, w - 1, -1):
                        if dp[c - w] + v > dp[c]:
                            dp[c] = dp[c - w] + v
                            keep[i][c] = True
                selected = []
                c = capacity
                for i in range(len(items) - 1, -1, -1):
                    if keep[i][c]:
                        selected.append(items[i])
                        c -= items[i]["fee_scaled"]

                if selected:
                    st.session_state.a3_result = selected
                else:
                    st.warning("예산 내에서 선택 가능한 KOL이 없습니다.")

            if st.session_state.get("a3_result"):
                sel = st.session_state.a3_result
                tc = sum(s["data"]["latest_fee"] for s in sel)
                tv = sum(s["data"].get("latest_views") or 0 for s in sel)
                mc = st.columns(4)
                with mc[0]:
                    st.metric("선택 KOL", f"{len(sel)}명")
                with mc[1]:
                    st.metric("예상 비용", f"¥{int(tc):,}")
                with mc[2]:
                    st.metric("예상 노출", f"{int(tv):,}")
                with mc[3]:
                    st.metric("평균 CPV", f"¥{tc/tv:.2f}" if tv > 0 else "—")

                rows = []
                for s in sel:
                    k = s["data"]
                    v = k.get("latest_views") or 0
                    rows.append({
                        "KOL명": k["name"], "플랫폼": k["platform"],
                        "비용": f"¥{int(k['latest_fee']):,}",
                        "예상 조회수": f"{int(v):,}",
                        "점수": k.get("latest_score"),
                        "등급": k.get("latest_grade", "—"),
                        "기여도%": round(v / tv * 100, 1) if tv > 0 else 0,
                    })
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    # ══════════════════════════════════════════════════════════
    #  F5 + A4: 유가 포스팅 분석 + 소재 분석
    # ══════════════════════════════════════════════════════════
    with tool_tabs[2]:
        st.markdown("### 📈 유가 포스팅 성과 분석")
        f5_tab1, f5_tab2 = st.tabs(["📝 포스팅 등록", "📊 성과 리포트"])

        with f5_tab1:
            campaigns = get_campaigns()
            c1, c2 = st.columns([2, 1])
            with c1:
                new_cn = st.text_input("새 캠페인", placeholder="예: 2024 봄 선크림", key="f5_newc")
            with c2:
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("➕ 캠페인 생성", use_container_width=True, key="f5_cc") and new_cn.strip():
                    create_campaign(new_cn.strip())
                    st.success(f"✅ '{new_cn}' 생성됨")
                    st.rerun()

            if campaigns:
                sel_cn = st.selectbox("캠페인 선택", [c["campaign_name"] for c in campaigns], key="f5_sel")
                sel_cid = next(c["id"] for c in campaigns if c["campaign_name"] == sel_cn)

                if "f5_rows" not in st.session_state:
                    st.session_state.f5_rows = pd.DataFrame({
                        "KOL명": [""], "URL": [""], "비용(JPY)": [0],
                        "포스팅일": [""], "소재유형": [""],
                    })

                f5_edited = st.data_editor(
                    st.session_state.f5_rows, use_container_width=True,
                    num_rows="dynamic", hide_index=True,
                    column_config={
                        "URL": st.column_config.TextColumn("포스팅 URL", width="large"),
                        "비용(JPY)": st.column_config.NumberColumn("비용(JPY)", min_value=0, format="¥%d"),
                        "소재유형": st.column_config.SelectboxColumn("소재 유형", options=CONTENT_TYPES),
                    },
                    key="f5_editor",
                )
                st.session_state.f5_rows = f5_edited

                f5_valid = (f5_edited[f5_edited["URL"].str.strip().str.len() > 5]
                            if not f5_edited.empty else pd.DataFrame())

                if not f5_valid.empty and st.button("🚀 등록 + 스크래핑", type="primary",
                                                     use_container_width=True, key="f5_run"):
                    prog = st.progress(0)
                    browser_dir = Path.home() / ".kol_tool_session"
                    pw_ctx, _pw = None, None
                    if not IS_CLOUD:
                        try:
                            from playwright.sync_api import sync_playwright
                            _pw = sync_playwright().__enter__()
                            if browser_dir.exists():
                                pw_ctx = _pw.chromium.launch_persistent_context(
                                    str(browser_dir), headless=True)
                        except Exception:
                            pass

                    for i, (_, row) in enumerate(f5_valid.iterrows()):
                        url = str(row["URL"]).strip()
                        plat = detect_platform(url)
                        name = str(row.get("KOL명", "")).strip() or extract_username(url, plat) or url
                        fee = float(row.get("비용(JPY)", 0) or 0)
                        prog.progress((i + 0.5) / len(f5_valid), text=f"{name}...")
                        pid = add_paid_post(
                            sel_cid, name, plat, url,
                            str(row.get("포스팅일", "")).strip(),
                            fee, str(row.get("소재유형", "")).strip(),
                        )
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
                            update_paid_post_metrics(
                                pid, raw.get("avg_views") or 0,
                                raw.get("avg_likes") or 0, raw.get("avg_comments") or 0,
                                raw.get("avg_saves") or 0, raw.get("avg_shares") or 0, fee,
                            )

                    if pw_ctx:
                        try:
                            pw_ctx.close()
                            _pw.__exit__(None, None, None)
                        except Exception:
                            pass
                    prog.progress(1.0, text="✅ 완료!")
                    st.rerun()

        with f5_tab2:
            camp_list = get_campaigns()
            if not camp_list:
                st.info("캠페인이 없습니다.")
            else:
                rc_name = st.selectbox(
                    "캠페인", ["전체"] + [c["campaign_name"] for c in camp_list], key="f5_rpt")
                rc_id = (next((c["id"] for c in camp_list if c["campaign_name"] == rc_name), None)
                         if rc_name != "전체" else None)
                posts = get_paid_posts(rc_id)
                if not posts:
                    st.info("포스팅이 없습니다.")
                else:
                    pdf = pd.DataFrame(posts)
                    mc = st.columns(4)
                    with mc[0]:
                        st.metric("총 비용", f"¥{int(pdf['fee'].sum() or 0):,}")
                    with mc[1]:
                        st.metric("총 조회수", f"{int(pdf['views'].sum() or 0):,}")
                    with mc[2]:
                        acpv = pdf["cpv"].mean() if pdf["cpv"].notna().any() else None
                        st.metric("평균 CPV", f"¥{acpv:.2f}" if acpv else "—")
                    with mc[3]:
                        aer = pdf["er_pct"].mean() if pdf["er_pct"].notna().any() else None
                        st.metric("평균 ER", f"{aer:.2f}%" if aer else "—")

                    disp_cols = ["kol_name", "platform", "post_date", "content_type",
                                 "fee", "views", "likes", "comments", "saves",
                                 "cpv", "cpe", "er_pct"]
                    disp_cols = [c for c in disp_cols if c in pdf.columns]
                    dp = pdf[disp_cols].rename(columns={
                        "kol_name": "KOL명", "platform": "플랫폼", "post_date": "포스팅일",
                        "content_type": "소재유형", "fee": "비용", "views": "조회수",
                        "likes": "좋아요", "comments": "댓글", "saves": "저장",
                        "cpv": "CPV", "cpe": "CPE", "er_pct": "ER%",
                    })
                    st.dataframe(dp, use_container_width=True, hide_index=True, height=300)

                    # KOL별 CPV 차트
                    cpv_data = pdf[pdf["cpv"].notna()][["kol_name", "cpv"]]
                    if not cpv_data.empty:
                        st.markdown("#### KOL별 CPV 비교")
                        st.bar_chart(
                            cpv_data.rename(columns={"kol_name": "KOL명", "cpv": "CPV"}).set_index("KOL명"),
                            y="CPV",
                        )

                    # A4: 소재 분석
                    if "content_type" in pdf.columns:
                        tdata = pdf[pdf["content_type"].str.strip().ne("") & pdf["cpv"].notna()]
                        if not tdata.empty:
                            st.markdown("---")
                            st.markdown("#### 🎬 소재 유형별 분석")
                            ts = (tdata.groupby("content_type")
                                  .agg(포스팅수=("id", "count"), 평균CPV=("cpv", "mean"),
                                       평균ER=("er_pct", "mean"), 평균조회수=("views", "mean"))
                                  .round(2).reset_index()
                                  .rename(columns={"content_type": "소재유형"}))
                            ts["효율점수"] = ts.apply(
                                lambda r: round(r["평균ER"] / r["평균CPV"], 4)
                                if r["평균CPV"] > 0 else None, axis=1
                            )
                            ts = ts.sort_values("효율점수", ascending=False, na_position="last")
                            st.dataframe(ts, use_container_width=True, hide_index=True)

                            best = ts.iloc[0] if len(ts) > 0 else None
                            if best is not None:
                                st.markdown(
                                    f"💡 **추천 소재**: **{best['소재유형']}** "
                                    f"(평균 CPV ¥{best['평균CPV']:.2f}, ER {best['평균ER']:.2f}%)"
                                )

    # ══════════════════════════════════════════════════════════
    #  F6: 노출-쿼리 상관관계 분석
    # ══════════════════════════════════════════════════════════
    with tool_tabs[3]:
        st.markdown("### 📈 노출-쿼리 상관관계 분석")
        st.caption("유가 포스팅의 노출수 추이와 브랜드 검색량 변화의 상관관계를 분석합니다.")

        f6c1, f6c2 = st.columns(2)
        with f6c1:
            f6_kw = st.text_input("Google Trends 키워드",
                                  placeholder="예: ANESSA 日焼け止め", key="f6_kw")
        with f6c2:
            f6_cl = get_campaigns()
            f6_cn = st.selectbox(
                "캠페인", ["전체"] + [c["campaign_name"] for c in f6_cl], key="f6_cn")

        f6c3, f6c4 = st.columns(2)
        with f6c3:
            f6_manual = st.text_area("수동 검색량 (날짜,수치)", height=60, key="f6_manual",
                                     placeholder="2024-01-01,120")
        with f6c4:
            f6_tf = st.selectbox("분석 기간",
                                 ["최근 1개월", "최근 2개월", "최근 3개월"], index=1, key="f6_tf")

        if st.button("📊 상관분석 실행", type="primary", use_container_width=True, key="f6_run"):
            cid = None
            if f6_cn != "전체":
                cid = next((c["id"] for c in f6_cl if c["campaign_name"] == f6_cn), None)
            daily = get_daily_impressions(cid)

            if not daily:
                st.warning("포스팅 데이터가 없습니다. 유가 포스팅을 먼저 등록하세요.")
            else:
                imp_df = pd.DataFrame(daily)
                imp_df["post_date"] = pd.to_datetime(imp_df["post_date"], errors="coerce")
                imp_df = imp_df.dropna(subset=["post_date"]).sort_values("post_date")

                days_map = {"최근 1개월": 30, "최근 2개월": 60, "최근 3개월": 90}
                cutoff = pd.Timestamp.now() - timedelta(days=days_map.get(f6_tf, 60))
                imp_df = imp_df[imp_df["post_date"] >= cutoff]

                # Google Trends
                trends_df = None
                if f6_kw.strip():
                    try:
                        from pytrends.request import TrendReq
                        pt = TrendReq(hl="ja-JP", tz=540)
                        tf_m = {"최근 1개월": "today 1-m", "최근 2개월": "today 3-m",
                                "최근 3개월": "today 3-m"}
                        pt.build_payload([f6_kw.strip()],
                                         timeframe=tf_m.get(f6_tf, "today 3-m"), geo="JP")
                        trends_df = pt.interest_over_time()
                        if not trends_df.empty:
                            if "isPartial" in trends_df.columns:
                                trends_df = trends_df.drop(columns=["isPartial"])
                            trends_df = trends_df.reset_index()
                            trends_df.columns = ["date", "search_volume"]
                    except Exception as e:
                        st.warning(f"Google Trends 오류: {e}")

                # 수동 데이터
                manual_df = None
                if f6_manual.strip():
                    try:
                        rows = []
                        for line in f6_manual.strip().split("\n"):
                            parts = line.split(",")
                            if len(parts) >= 2:
                                rows.append({"date": parts[0].strip(),
                                             "search_volume": float(parts[1].strip())})
                        if rows:
                            manual_df = pd.DataFrame(rows)
                            manual_df["date"] = pd.to_datetime(manual_df["date"], errors="coerce")
                    except Exception:
                        pass

                query_df = (trends_df if trends_df is not None and not trends_df.empty
                            else manual_df)

                chart_imp = imp_df[["post_date", "daily_views"]].rename(
                    columns={"post_date": "date", "daily_views": "노출수"})
                chart_imp["date"] = pd.to_datetime(chart_imp["date"])

                if query_df is not None and not query_df.empty:
                    query_df["date"] = pd.to_datetime(query_df["date"])
                    merged = (pd.merge(chart_imp, query_df, on="date", how="outer")
                              .sort_values("date").fillna(0))
                    mx_i = merged["노출수"].max() or 1
                    mx_q = merged["search_volume"].max() or 1
                    merged["노출수(정규화)"] = merged["노출수"] / mx_i * 100
                    merged["검색량(정규화)"] = merged["search_volume"] / mx_q * 100

                    st.markdown("#### 노출수 vs 검색량 추이")
                    st.line_chart(merged.set_index("date")[["노출수(정규화)", "검색량(정규화)"]])

                    valid = merged[(merged["노출수"] > 0) & (merged["search_volume"] > 0)]
                    if len(valid) >= 5:
                        corr = valid["노출수"].corr(valid["search_volume"])
                        strength = ("강한" if abs(corr) >= 0.7
                                    else "중간" if abs(corr) >= 0.4 else "약한")
                        st.metric("피어슨 상관계수", f"{corr:.3f}")
                        st.markdown(f"**{strength} {'양의' if corr > 0 else '음의'} 상관관계**")

                        lag_r = []
                        for lag in range(8):
                            sh = merged.copy()
                            sh["sq"] = sh["search_volume"].shift(-lag)
                            v2 = sh.dropna(subset=["노출수", "sq"])
                            v2 = v2[(v2["노출수"] > 0) & (v2["sq"] > 0)]
                            if len(v2) >= 3:
                                lag_r.append({
                                    "시차(일)": lag,
                                    "상관계수": round(v2["노출수"].corr(v2["sq"]), 3),
                                })
                        if lag_r:
                            st.dataframe(pd.DataFrame(lag_r),
                                         use_container_width=True, hide_index=True)
                            bl = max(lag_r, key=lambda x: abs(x["상관계수"]))
                            if bl["시차(일)"] > 0:
                                st.markdown(
                                    f"→ 노출 후 약 **{bl['시차(일)']}일** 뒤 검색량 변화 최대")
                    else:
                        st.info("상관 분석을 위한 데이터가 부족합니다 (최소 5일치).")
                else:
                    st.line_chart(chart_imp.set_index("date"), y="노출수")
                    st.info("검색량 데이터를 입력하면 상관분석이 가능합니다.")
