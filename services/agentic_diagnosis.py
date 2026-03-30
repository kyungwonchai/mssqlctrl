import json
import os
from typing import Any, Dict, Optional

from services.llm_client import chat_completions


def _conn_slug(name: str) -> str:
    return name.replace(" ", "_")


def _read_text(path: str, limit: int) -> str:
    if not os.path.isfile(path):
        return ""
    with open(path, encoding="utf-8") as f:
        return f.read(limit)


def build_diagnosis_context(
    export_root: str,
    connection_name: str,
    database_name: str,
    live_snapshot: Optional[Dict[str, Any]] = None,
    max_total_chars: int = 28000,
    max_tables: int = 45,
    ddl_cap: int = 1800,
) -> str:
    slug = _conn_slug(connection_name)
    db_path = os.path.join(export_root, slug, database_name)
    tables_dir = os.path.join(db_path, "tables")
    meta_path = os.path.join(db_path, "db_metadata.json")

    parts: list[str] = []
    if live_snapshot:
        parts.append(
            "### 실시간 스냅샷 (진단 시점 DB 쿼리)\n"
            + json.dumps(live_snapshot, ensure_ascii=False, indent=2)[:14000]
        )
    elif os.path.isfile(meta_path):
        parts.append(
            "### 추출 저장 메타데이터 (용량·행수)\n" + _read_text(meta_path, 14000)
        )

    if os.path.isdir(tables_dir):
        schemas = sorted(
            f for f in os.listdir(tables_dir) if f.endswith("_schema.sql")
        )[:max_tables]
        for fn in schemas:
            ddl = _read_text(os.path.join(tables_dir, fn), ddl_cap)
            if ddl.strip():
                parts.append(f"### DDL {fn.replace('_schema.sql', '')}\n{ddl}")

    text = "\n\n".join(parts)
    if len(text) > max_total_chars:
        return text[:max_total_chars] + "\n\n…(잘림: 토큰 한도)"
    return text


SYSTEM_ANALYST = """당신은 MS SQL / MySQL 인프라를 다루는 시니어 DBA입니다.
출력은 반드시 한국어로 작성합니다. 추측은 근거와 함께 적고, 모르면 모른다고 합니다."""


def run_agentic_diagnosis(
    context: str,
    llm_base_url: str,
    llm_model: str,
    api_key: Optional[str] = None,
) -> str:
    """
    2단계 에이전트: (1) 짧은 가설·위험 목록 (2) 정식 진단 리포트.
    """
    step1 = chat_completions(
        llm_base_url,
        llm_model,
        messages=[
            {"role": "system", "content": SYSTEM_ANALYST},
            {
                "role": "user",
                "content": (
                    "아래는 추출된 DDL 요약과 테이블 용량/행수 메타입니다.\n"
                    "Qwen이 분석하기 쉽게, 먼저 **3~8개 불릿**으로 잠재 문제 가설을 적으세요. "
                    "예: 데이터 급증 징후, 인덱스 부족 가능성, 스키마 설계 리스크, 파티셔닝·보관 주기 등.\n\n"
                    f"{context}"
                ),
            },
        ],
        api_key=api_key,
        temperature=0.25,
    )

    step2 = chat_completions(
        llm_base_url,
        llm_model,
        messages=[
            {"role": "system", "content": SYSTEM_ANALYST},
            {
                "role": "user",
                "content": (
                    f"### 1차 관찰 (에이전트)\n{step1}\n\n"
                    "### 원본 컨텍스트 (일부)\n"
                    f"{context[:12000]}\n\n"
                    "위를 **통합**하여 아래 **마크다운 구조**로 기업용 진단 보고서를 작성하세요.\n"
                    "## 1. 요약\n"
                    "## 2. 용량·데이터 증가 관점\n"
                    "## 3. 인덱스·쿼리·접근 패턴 (추정)\n"
                    "## 4. 즉시 조치 / 중기 개선 제안\n"
                    "## 5. 추가로 수집하면 좋은 지표 (Extended Events, 느린 쿼리 로그 등)\n"
                ),
            },
        ],
        api_key=api_key,
        temperature=0.35,
    )

    return (
        "## 에이전트 1차 관찰\n\n"
        + step1.strip()
        + "\n\n---\n\n## AI DB 진단 리포트\n\n"
        + step2.strip()
    )
