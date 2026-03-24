"""
KOL 선별 프레임워크 - 점수 계산 모듈
플랫폼별 가중치 적용 + 백분위 랭크 → 1~10점 + 5단계 등급
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional


# ──────────────────────────────────────────────────────────────
#  데이터 구조
# ──────────────────────────────────────────────────────────────

@dataclass
class KOLMetrics:
    name:     str
    platform: str
    url:      str
    fee:      Optional[float] = None   # 캐스팅 비용 (JPY)

    # 스크래핑 결과 (평균값)
    avg_views:    Optional[float] = None
    avg_likes:    Optional[float] = None
    avg_comments: Optional[float] = None
    avg_saves:    Optional[float] = None
    avg_shares:   Optional[float] = None
    post_count:   int = 0

    # 계산된 지표
    cpv:           Optional[float] = None  # 조회당 비용
    er_pct:        Optional[float] = None  # 참여율 %
    save_rate_pct: Optional[float] = None  # 저장률 %
    cpe:           Optional[float] = None  # 참여당 비용
    save_ratio_pct:Optional[float] = None  # 저장비율 % (Lipscosme)
    comment_ratio_pct:Optional[float] = None  # 댓글비율 % (Instagram)

    # 점수
    score:  Optional[float] = None
    grade:  str = ""
    adopt:  str = ""  # 채택 권고 여부


# ──────────────────────────────────────────────────────────────
#  지표 계산
# ──────────────────────────────────────────────────────────────

def compute_metrics(kol: KOLMetrics) -> KOLMetrics:
    """원시 데이터 → 파생 지표 계산"""
    fee   = kol.fee
    views = kol.avg_views
    likes = kol.avg_likes
    comments = kol.avg_comments
    saves = kol.avg_saves
    shares = kol.avg_shares

    total_eng = sum(
        v for v in [likes, comments, saves, shares] if v is not None
    ) or None

    # CPV (비용 / 조회수)
    if fee and views and views > 0:
        kol.cpv = round(fee / views, 2)

    # ER% ((좋아요+댓글+저장+공유) / 조회수 × 100)
    if total_eng is not None and views and views > 0:
        kol.er_pct = round(total_eng / views * 100, 2)

    # 저장률% (저장 / 조회수 × 100)
    if saves is not None and views and views > 0:
        kol.save_rate_pct = round(saves / views * 100, 2)

    # CPE (비용 / 총 참여)
    if fee and total_eng and total_eng > 0:
        kol.cpe = round(fee / total_eng, 1)

    # 저장비율% = 저장 / (좋아요+댓글+저장) × 100  [Lipscosme]
    denom = sum(v for v in [likes, comments, saves] if v is not None) or None
    if saves is not None and denom and denom > 0:
        kol.save_ratio_pct = round(saves / denom * 100, 1)

    # 댓글비율% = 댓글 / 총 참여 × 100  [Instagram]
    if comments is not None and total_eng and total_eng > 0:
        kol.comment_ratio_pct = round(comments / total_eng * 100, 1)

    return kol


# ──────────────────────────────────────────────────────────────
#  백분위 랭크 점수화
# ──────────────────────────────────────────────────────────────

def _percentile_score(values: list[tuple], higher_is_better: bool) -> dict:
    """
    [(key, value), ...] → {key: 1~10 점}
    None 값은 점수 0 처리
    """
    valid = [(k, v) for k, v in values if v is not None]
    n = len(valid)
    if n == 0:
        return {k: None for k, v in values}

    sorted_valid = sorted(valid, key=lambda x: x[1], reverse=higher_is_better)
    scores = {}
    for rank, (k, _) in enumerate(sorted_valid):
        scores[k] = round((n - rank) / n * 10, 2)

    # None인 항목은 None 점수
    for k, v in values:
        if v is None:
            scores.setdefault(k, None)
    return scores


# ──────────────────────────────────────────────────────────────
#  플랫폼별 가중치 정의
# ──────────────────────────────────────────────────────────────

PLATFORM_WEIGHTS = {
    "TikTok": [
        ("cpv",           False, 0.35),   # 낮을수록 좋음
        ("er_pct",        True,  0.35),   # 높을수록 좋음
        ("save_rate_pct", True,  0.30),
    ],
    "Instagram": [
        ("cpe",              False, 0.60),
        ("comment_ratio_pct",True,  0.40),
    ],
    "YouTube": [
        ("cpv", False, 1.00),
    ],
    "Twitter": [
        ("cpv",    False, 0.50),
        ("er_pct", True,  0.30),
        ("cpe",    False, 0.20),
    ],
    "Lipscosme": [
        ("cpe",            False, 0.50),
        ("save_ratio_pct", True,  0.50),
    ],
}


# ──────────────────────────────────────────────────────────────
#  등급 부여
# ──────────────────────────────────────────────────────────────

def _grade(score: float | None) -> tuple[str, str]:
    """점수 → (등급 문자열, 채택 권고)"""
    if score is None:
        return "—", "데이터 부족"
    if score >= 8.5:
        return "★★★★★", "✅ 최우선 채택"
    if score >= 7.0:
        return "★★★★",  "✅ 채택 권고"
    if score >= 5.5:
        return "★★★",   "🔶 조건부 채택"
    if score >= 3.0:
        return "★★",    "⏸ 보류"
    return "★", "❌ 비권고"


# ──────────────────────────────────────────────────────────────
#  전체 KOL 목록 점수 계산
# ──────────────────────────────────────────────────────────────

def calculate_all_scores(kols: list[KOLMetrics]) -> list[KOLMetrics]:
    """
    1. 각 KOL의 파생 지표 계산
    2. 동일 플랫폼 내 백분위 랭크 산출
    3. 가중 합산 → 종합 점수
    4. 등급 부여
    """
    # ── Step 1: 파생 지표 계산 ──
    for kol in kols:
        compute_metrics(kol)

    # ── Step 2~3: 플랫폼 그룹별 점수 ──
    platforms = set(kol.platform for kol in kols)
    for plat in platforms:
        group = [kol for kol in kols if kol.platform == plat]
        weights = PLATFORM_WEIGHTS.get(plat, [])
        if not weights:
            continue

        # 각 지표별 백분위 점수 산출
        metric_scores: dict[str, dict] = {}
        for metric, higher_is_better, _ in weights:
            vals = [(kol.name, getattr(kol, metric, None)) for kol in group]
            metric_scores[metric] = _percentile_score(vals, higher_is_better)

        # 가중 합산
        for kol in group:
            weighted_sum = 0.0
            total_weight  = 0.0
            for metric, _, weight in weights:
                s = metric_scores[metric].get(kol.name)
                if s is not None:
                    weighted_sum += s * weight
                    total_weight += weight

            if total_weight > 0:
                kol.score = round(weighted_sum / total_weight, 1)
            else:
                kol.score = None

            kol.grade, kol.adopt = _grade(kol.score)

    return kols


# ──────────────────────────────────────────────────────────────
#  DataFrame 변환 유틸
# ──────────────────────────────────────────────────────────────

def kols_to_dataframe(kols: list[KOLMetrics]):
    """KOLMetrics 리스트 → pandas DataFrame"""
    import pandas as pd

    rows = []
    for k in kols:
        rows.append({
            "KOL명":      k.name,
            "플랫폼":     k.platform,
            "URL":        k.url,
            "비용(JPY)":  k.fee,
            "분석게시물수": k.post_count,
            "평균 조회수": k.avg_views,
            "평균 좋아요": k.avg_likes,
            "평균 댓글":   k.avg_comments,
            "평균 저장":   k.avg_saves,
            "평균 공유":   k.avg_shares,
            "CPV(¥/회)":  k.cpv,
            "ER%":         k.er_pct,
            "저장률%":     k.save_rate_pct,
            "CPE(¥/건)":  k.cpe,
            "저장비율%":   k.save_ratio_pct,
            "댓글비율%":   k.comment_ratio_pct,
            "종합점수":    k.score,
            "등급":        k.grade,
            "채택권고":    k.adopt,
        })
    return pd.DataFrame(rows)


# ──────────────────────────────────────────────────────────────
#  오디언스 품질 분석 (A2)
# ──────────────────────────────────────────────────────────────

def analyze_audience_quality(kols: list[KOLMetrics]) -> dict[str, list[str]]:
    """
    KOL 목록을 분석해 품질 경고를 반환.
    반환: {kol.name: [경고 메시지, ...]}
    """
    import statistics as _stats

    warnings: dict[str, list[str]] = {}

    # 플랫폼별 그룹화해서 평균 ER 계산
    plat_groups: dict[str, list[KOLMetrics]] = {}
    for k in kols:
        plat_groups.setdefault(k.platform, []).append(k)

    plat_avg_er: dict[str, float] = {}
    for plat, group in plat_groups.items():
        er_vals = [k.er_pct for k in group if k.er_pct is not None]
        if er_vals:
            plat_avg_er[plat] = _stats.mean(er_vals)

    for k in kols:
        flags = []

        # 1. ER이 동일 플랫폼 평균의 20% 이하 → 저품질 의심
        avg_er = plat_avg_er.get(k.platform)
        if avg_er and k.er_pct is not None:
            if k.er_pct <= avg_er * 0.2:
                flags.append(
                    f"⚠️ 저품질 의심: ER {k.er_pct:.2f}%로 "
                    f"동일 플랫폼 평균({avg_er:.2f}%)의 20% 이하"
                )

        # 2. 조회수 대비 좋아요가 극단적으로 낮음 → 구매 팔로워 의심
        if k.avg_views and k.avg_likes is not None and k.avg_views > 0:
            like_rate = k.avg_likes / k.avg_views
            if like_rate < 0.005:  # 0.5% 미만
                flags.append(
                    f"🚩 구매 팔로워 의심: 좋아요율 {like_rate*100:.2f}% "
                    f"(조회수 대비 극단적으로 낮음)"
                )

        # 3. 댓글이 지나치게 적음 (조회수 있는데 댓글 0~1)
        if k.avg_views and k.avg_views > 1000:
            if k.avg_comments is not None and k.avg_comments <= 1:
                flags.append(
                    "💬 댓글 부족: 조회수 대비 댓글이 거의 없음 "
                    "(봇 트래픽 또는 참여도 저조 가능성)"
                )

        # 4. 저장/공유 없이 좋아요만 높음 → 낮은 구매전환 가능성
        if k.avg_likes and k.avg_likes > 100:
            saves = k.avg_saves or 0
            if saves == 0 and k.platform in ("TikTok", "Instagram"):
                flags.append(
                    "📌 저장 0건: 좋아요는 있으나 저장이 전무 "
                    "(구매 전환 가능성 낮음)"
                )

        if flags:
            warnings[k.name] = flags

    return warnings
