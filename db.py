"""
KOL 히스토리 DB — SQLite 기반 KOL 포트폴리오 관리
"""
from __future__ import annotations

import sqlite3
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

DB_PATH = Path.home() / ".kol_tool_data" / "kol_history.db"


def _get_conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    """테이블 생성 (최초 1회)"""
    conn = _get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS kols (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            name            TEXT NOT NULL,
            platform        TEXT NOT NULL,
            url             TEXT NOT NULL UNIQUE,
            first_scored_at TEXT NOT NULL,
            last_updated_at TEXT NOT NULL,
            campaign_status TEXT DEFAULT '미접촉',
            memo            TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS kol_snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            kol_id          INTEGER NOT NULL REFERENCES kols(id),
            snapshot_at     TEXT NOT NULL,
            fee             REAL,
            avg_views       REAL,
            avg_likes       REAL,
            avg_comments    REAL,
            avg_saves       REAL,
            avg_shares      REAL,
            post_count      INTEGER,
            cpv             REAL,
            er_pct          REAL,
            save_rate_pct   REAL,
            cpe             REAL,
            save_ratio_pct  REAL,
            comment_ratio_pct REAL,
            score           REAL,
            grade           TEXT,
            adopt           TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_snapshots_kol ON kol_snapshots(kol_id);
        CREATE INDEX IF NOT EXISTS idx_snapshots_at  ON kol_snapshots(snapshot_at);

        CREATE TABLE IF NOT EXISTS campaigns (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            campaign_name   TEXT NOT NULL,
            created_at      TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS paid_posts (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            campaign_id     INTEGER REFERENCES campaigns(id),
            kol_name        TEXT NOT NULL,
            platform        TEXT NOT NULL,
            url             TEXT NOT NULL,
            post_date       TEXT,
            fee             REAL,
            content_type    TEXT DEFAULT '',
            views           REAL,
            likes           REAL,
            comments        REAL,
            saves           REAL,
            shares          REAL,
            cpv             REAL,
            cpe             REAL,
            er_pct          REAL,
            scraped_at      TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_paid_campaign ON paid_posts(campaign_id);
    """)
    conn.commit()
    conn.close()


def upsert_kol(name: str, platform: str, url: str,
               campaign_status: str = None, memo: str = None) -> int:
    """KOL 레코드 생성 또는 업데이트. kol_id 반환."""
    conn = _get_conn()
    now = datetime.now().isoformat(timespec="seconds")

    row = conn.execute("SELECT id, campaign_status, memo FROM kols WHERE url = ?", (url,)).fetchone()
    if row:
        kol_id = row["id"]
        conn.execute(
            "UPDATE kols SET name=?, platform=?, last_updated_at=?, "
            "campaign_status=COALESCE(?, campaign_status), "
            "memo=COALESCE(?, memo) "
            "WHERE id=?",
            (name, platform, now, campaign_status, memo, kol_id)
        )
    else:
        cur = conn.execute(
            "INSERT INTO kols (name, platform, url, first_scored_at, last_updated_at, "
            "campaign_status, memo) VALUES (?,?,?,?,?,?,?)",
            (name, platform, url, now, now, campaign_status or "미접촉", memo or "")
        )
        kol_id = cur.lastrowid

    conn.commit()
    conn.close()
    return kol_id


def add_snapshot(kol_id: int, metrics: dict):
    """KOL 스냅샷(지표 기록) 추가"""
    conn = _get_conn()
    now = datetime.now().isoformat(timespec="seconds")
    conn.execute(
        "INSERT INTO kol_snapshots "
        "(kol_id, snapshot_at, fee, avg_views, avg_likes, avg_comments, "
        "avg_saves, avg_shares, post_count, cpv, er_pct, save_rate_pct, "
        "cpe, save_ratio_pct, comment_ratio_pct, score, grade, adopt) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            kol_id, now,
            metrics.get("fee"), metrics.get("avg_views"), metrics.get("avg_likes"),
            metrics.get("avg_comments"), metrics.get("avg_saves"), metrics.get("avg_shares"),
            metrics.get("post_count"), metrics.get("cpv"), metrics.get("er_pct"),
            metrics.get("save_rate_pct"), metrics.get("cpe"),
            metrics.get("save_ratio_pct"), metrics.get("comment_ratio_pct"),
            metrics.get("score"), metrics.get("grade"), metrics.get("adopt"),
        )
    )
    conn.commit()
    conn.close()


def save_scored_df(df) -> int:
    """스코어카드 DataFrame을 DB에 일괄 저장. 저장 건수 반환."""
    import pandas as pd
    count = 0
    for _, row in df.iterrows():
        url = str(row.get("URL", "")).strip()
        if not url:
            continue
        kol_id = upsert_kol(
            name=str(row.get("KOL명", "")),
            platform=str(row.get("플랫폼", "")),
            url=url,
        )
        add_snapshot(kol_id, {
            "fee":             row.get("비용(JPY)"),
            "avg_views":       row.get("평균 조회수"),
            "avg_likes":       row.get("평균 좋아요"),
            "avg_comments":    row.get("평균 댓글"),
            "avg_saves":       row.get("평균 저장"),
            "avg_shares":      row.get("평균 공유"),
            "post_count":      row.get("분석게시물수"),
            "cpv":             row.get("CPV(¥/회)"),
            "er_pct":          row.get("ER%"),
            "save_rate_pct":   row.get("저장률%"),
            "cpe":             row.get("CPE(¥/건)"),
            "save_ratio_pct":  row.get("저장비율%"),
            "comment_ratio_pct": row.get("댓글비율%"),
            "score":           row.get("종합점수"),
            "grade":           row.get("등급"),
            "adopt":           row.get("채택권고"),
        })
        count += 1
    return count


def get_all_kols() -> list[dict]:
    """전체 KOL 목록 + 최신 스냅샷 요약"""
    conn = _get_conn()
    rows = conn.execute("""
        SELECT k.*,
               s.score AS latest_score,
               s.grade AS latest_grade,
               s.er_pct AS latest_er,
               s.avg_views AS latest_views,
               s.fee AS latest_fee,
               s.snapshot_at AS latest_snapshot_at
        FROM kols k
        LEFT JOIN kol_snapshots s ON s.id = (
            SELECT id FROM kol_snapshots
            WHERE kol_id = k.id
            ORDER BY snapshot_at DESC LIMIT 1
        )
        ORDER BY k.last_updated_at DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_kol_history(kol_id: int) -> list[dict]:
    """특정 KOL의 전체 스냅샷 이력"""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM kol_snapshots WHERE kol_id = ? ORDER BY snapshot_at DESC",
        (kol_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_kol_delta(kol_id: int) -> Optional[dict]:
    """최근 2회 스냅샷 비교 (ER 변화, 조회수 변화 등)"""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM kol_snapshots WHERE kol_id = ? ORDER BY snapshot_at DESC LIMIT 2",
        (kol_id,)
    ).fetchall()
    conn.close()
    if len(rows) < 2:
        return None
    curr, prev = dict(rows[0]), dict(rows[1])
    delta = {}
    for key in ("score", "er_pct", "avg_views", "avg_likes", "cpv", "cpe"):
        c, p = curr.get(key), prev.get(key)
        if c is not None and p is not None:
            delta[key] = round(c - p, 2)
        else:
            delta[key] = None
    delta["prev_date"] = prev.get("snapshot_at", "")
    delta["curr_date"] = curr.get("snapshot_at", "")
    return delta


def update_campaign_status(kol_id: int, status: str):
    """캠페인 상태 업데이트"""
    conn = _get_conn()
    conn.execute("UPDATE kols SET campaign_status=? WHERE id=?", (status, kol_id))
    conn.commit()
    conn.close()


def update_memo(kol_id: int, memo: str):
    """메모 업데이트"""
    conn = _get_conn()
    conn.execute("UPDATE kols SET memo=? WHERE id=?", (memo, kol_id))
    conn.commit()
    conn.close()


def delete_kol(kol_id: int):
    """KOL 및 관련 스냅샷 삭제"""
    conn = _get_conn()
    conn.execute("DELETE FROM kol_snapshots WHERE kol_id=?", (kol_id,))
    conn.execute("DELETE FROM kols WHERE id=?", (kol_id,))
    conn.commit()
    conn.close()


def get_db_stats() -> dict:
    """DB 통계"""
    conn = _get_conn()
    kol_count = conn.execute("SELECT COUNT(*) FROM kols").fetchone()[0]
    snapshot_count = conn.execute("SELECT COUNT(*) FROM kol_snapshots").fetchone()[0]
    platforms = conn.execute(
        "SELECT platform, COUNT(*) as cnt FROM kols GROUP BY platform ORDER BY cnt DESC"
    ).fetchall()
    conn.close()
    return {
        "kol_count": kol_count,
        "snapshot_count": snapshot_count,
        "platforms": {r["platform"]: r["cnt"] for r in platforms},
    }


# ──────────────────────────────────────────────────────────────
#  캠페인 / 유가 포스팅 관련
# ──────────────────────────────────────────────────────────────

def create_campaign(name: str) -> int:
    """캠페인 생성, campaign_id 반환"""
    conn = _get_conn()
    now = datetime.now().isoformat(timespec="seconds")
    cur = conn.execute(
        "INSERT INTO campaigns (campaign_name, created_at) VALUES (?,?)",
        (name, now)
    )
    conn.commit()
    cid = cur.lastrowid
    conn.close()
    return cid


def get_campaigns() -> list[dict]:
    """전체 캠페인 목록"""
    conn = _get_conn()
    rows = conn.execute("""
        SELECT c.*, COUNT(p.id) as post_count,
               COALESCE(SUM(p.fee), 0) as total_fee,
               COALESCE(SUM(p.views), 0) as total_views
        FROM campaigns c
        LEFT JOIN paid_posts p ON p.campaign_id = c.id
        GROUP BY c.id
        ORDER BY c.created_at DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def add_paid_post(campaign_id: int, kol_name: str, platform: str,
                  url: str, post_date: str, fee: float,
                  content_type: str = "") -> int:
    """유가 포스팅 등록, post_id 반환"""
    conn = _get_conn()
    cur = conn.execute(
        "INSERT INTO paid_posts "
        "(campaign_id, kol_name, platform, url, post_date, fee, content_type) "
        "VALUES (?,?,?,?,?,?,?)",
        (campaign_id, kol_name, platform, url, post_date, fee, content_type)
    )
    conn.commit()
    pid = cur.lastrowid
    conn.close()
    return pid


def update_paid_post_metrics(post_id: int, views: float, likes: float,
                              comments: float, saves: float, shares: float,
                              fee: float):
    """유가 포스팅의 스크래핑 결과 + 성과 지표 업데이트"""
    now = datetime.now().isoformat(timespec="seconds")
    total_eng = sum(v for v in [likes, comments, saves, shares] if v) or 0
    cpv = round(fee / views, 2) if fee and views else None
    cpe = round(fee / total_eng, 1) if fee and total_eng else None
    er = round(total_eng / views * 100, 2) if views else None

    conn = _get_conn()
    conn.execute(
        "UPDATE paid_posts SET views=?, likes=?, comments=?, saves=?, shares=?, "
        "cpv=?, cpe=?, er_pct=?, scraped_at=? WHERE id=?",
        (views, likes, comments, saves, shares, cpv, cpe, er, now, post_id)
    )
    conn.commit()
    conn.close()
    return {"cpv": cpv, "cpe": cpe, "er_pct": er}


def get_paid_posts(campaign_id: int = None) -> list[dict]:
    """유가 포스팅 목록 (캠페인별 또는 전체)"""
    conn = _get_conn()
    if campaign_id:
        rows = conn.execute(
            "SELECT p.*, c.campaign_name FROM paid_posts p "
            "LEFT JOIN campaigns c ON c.id = p.campaign_id "
            "WHERE p.campaign_id = ? ORDER BY p.post_date DESC",
            (campaign_id,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT p.*, c.campaign_name FROM paid_posts p "
            "LEFT JOIN campaigns c ON c.id = p.campaign_id "
            "ORDER BY p.post_date DESC"
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_campaign_summary(campaign_id: int) -> dict:
    """캠페인 성과 요약"""
    conn = _get_conn()
    row = conn.execute("""
        SELECT
            COUNT(*) as post_count,
            SUM(fee) as total_fee,
            SUM(views) as total_views,
            SUM(likes) as total_likes,
            SUM(comments) as total_comments,
            SUM(saves) as total_saves,
            AVG(cpv) as avg_cpv,
            AVG(cpe) as avg_cpe,
            AVG(er_pct) as avg_er
        FROM paid_posts WHERE campaign_id = ?
    """, (campaign_id,)).fetchone()
    conn.close()
    return dict(row) if row else {}


def delete_campaign(campaign_id: int):
    """캠페인 및 관련 포스팅 삭제"""
    conn = _get_conn()
    conn.execute("DELETE FROM paid_posts WHERE campaign_id=?", (campaign_id,))
    conn.execute("DELETE FROM campaigns WHERE id=?", (campaign_id,))
    conn.commit()
    conn.close()


def get_daily_impressions(campaign_id: int = None) -> list[dict]:
    """유가 포스팅의 포스팅일 기준 일별 노출수 합산"""
    conn = _get_conn()
    where = "WHERE campaign_id = ?" if campaign_id else ""
    params = (campaign_id,) if campaign_id else ()
    rows = conn.execute(f"""
        SELECT post_date, SUM(views) as daily_views, COUNT(*) as post_count
        FROM paid_posts
        {where}
        AND post_date IS NOT NULL AND post_date != ''
        GROUP BY post_date
        ORDER BY post_date
    """, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]
