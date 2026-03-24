"""
KOL 히스토리 DB 페이지
"""

import streamlit as st
import pandas as pd

from db import (
    get_all_kols, get_kol_history, get_kol_delta,
    update_campaign_status, update_memo, get_db_stats,
)

CAMPAIGN_STATUSES = ["미접촉", "컨택 중", "진행 중", "완료", "보류"]


def render_history_page():
    st.markdown("### 📂 KOL 히스토리 DB")
    st.caption("스코어링한 KOL 데이터를 포트폴리오로 관리합니다.")

    stats = get_db_stats()
    if stats["kol_count"] == 0:
        st.info("아직 저장된 KOL이 없습니다. 스코어카드를 실행하면 자동 저장됩니다.")
        return

    # 통계 요약
    sc = st.columns(3)
    with sc[0]:
        st.metric("등록 KOL", f"{stats['kol_count']}명")
    with sc[1]:
        st.metric("스냅샷", f"{stats['snapshot_count']}건")
    with sc[2]:
        ps = " / ".join(f"{p} {c}" for p, c in stats["platforms"].items())
        st.metric("플랫폼", ps)

    all_kols = get_all_kols()
    kdf = pd.DataFrame(all_kols)

    # 필터
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        pf = st.multiselect("플랫폼", sorted(kdf["platform"].unique().tolist()), key="db_pf")
    with fc2:
        sf = st.multiselect("상태", CAMPAIGN_STATUSES, key="db_sf")
    with fc3:
        sq = st.text_input("KOL명 검색", key="db_sq")

    filtered = kdf.copy()
    if pf:
        filtered = filtered[filtered["platform"].isin(pf)]
    if sf:
        filtered = filtered[filtered["campaign_status"].isin(sf)]
    if sq:
        filtered = filtered[filtered["name"].str.contains(sq, case=False, na=False)]

    if filtered.empty:
        st.info("조건에 맞는 KOL이 없습니다.")
        return

    # 목록 테이블
    disp = filtered.rename(columns={
        "name": "KOL명", "platform": "플랫폼", "campaign_status": "상태",
        "latest_score": "점수", "latest_grade": "등급", "latest_er": "ER%",
        "latest_views": "평균 조회수", "first_scored_at": "최초 등록",
        "last_updated_at": "마지막 업데이트", "memo": "메모",
    })
    show = ["KOL명", "플랫폼", "상태", "점수", "등급", "ER%",
            "평균 조회수", "최초 등록", "마지막 업데이트", "메모"]
    show = [c for c in show if c in disp.columns]
    st.dataframe(
        disp[show].sort_values("점수", ascending=False, na_position="last"),
        use_container_width=True, hide_index=True, height=300,
    )

    # KOL 상세
    kol_names = filtered["name"].tolist()
    sel_kol = st.selectbox("KOL 상세 보기", ["선택하세요"] + kol_names, key="db_detail")

    if sel_kol == "선택하세요":
        return

    kr = filtered[filtered["name"] == sel_kol].iloc[0]
    kid = int(kr["id"])

    dc1, dc2 = st.columns([2, 1])
    with dc1:
        st.markdown(f"**{sel_kol}** ({kr['platform']}) — [{kr['url']}]({kr['url']})")

        delta = get_kol_delta(kid)
        if delta:
            items = []
            for key, label in [("score", "점수"), ("er_pct", "ER%"), ("avg_views", "조회수")]:
                d = delta.get(key)
                if d is not None:
                    items.append(f"{label}: {'+' if d > 0 else ''}{d}")
            if items:
                st.caption(f"지난 측정 대비 변화: {' | '.join(items)}")

        hist = get_kol_history(kid)
        if hist:
            hdf = pd.DataFrame(hist)
            h_cols = ["snapshot_at", "score", "grade", "er_pct", "avg_views", "cpv", "fee"]
            h_cols = [c for c in h_cols if c in hdf.columns]
            st.dataframe(
                hdf[h_cols].rename(columns={
                    "snapshot_at": "측정일", "score": "점수", "grade": "등급",
                    "er_pct": "ER%", "avg_views": "조회수", "cpv": "CPV", "fee": "비용"
                }),
                use_container_width=True, hide_index=True, height=200,
            )

    with dc2:
        cur_st = kr.get("campaign_status", "미접촉")
        new_st = st.selectbox(
            "캠페인 상태 변경", CAMPAIGN_STATUSES,
            index=CAMPAIGN_STATUSES.index(cur_st) if cur_st in CAMPAIGN_STATUSES else 0,
            key=f"st_{kid}",
        )
        if new_st != cur_st:
            if st.button("상태 저장", key=f"ss_{kid}"):
                update_campaign_status(kid, new_st)
                st.success(f"✅ {sel_kol} → {new_st}")
                st.rerun()

        cur_memo = kr.get("memo", "") or ""
        new_memo = st.text_area("메모", value=cur_memo, key=f"mm_{kid}", height=80)
        if new_memo != cur_memo:
            if st.button("메모 저장", key=f"sm_{kid}"):
                update_memo(kid, new_memo)
                st.success("✅ 메모 저장됨")
                st.rerun()
