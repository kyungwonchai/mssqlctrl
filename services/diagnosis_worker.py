"""
에이전틱 진단 백그라운드 실행. GPU/LLM 부하를 막기 위해 한 번에 하나의 리포트만 순차 처리한다.
반드시 Flask app.app_context() 안에서 호출한다.
"""

from datetime import datetime
from typing import List

from models import db, DBConnection, DiagnosisReport
from services.agentic_diagnosis import build_diagnosis_context, run_agentic_diagnosis
from services.sql_extractor import get_extractor


def execute_diagnosis_report(
    export_root: str,
    report_id: int,
    conn_id: int,
    database_name: str,
    use_live: bool,
    llm_base: str,
    llm_model: str,
    api_key,
) -> None:
    report = DiagnosisReport.query.get(report_id)
    conn = DBConnection.query.get(conn_id)
    if not report or not conn:
        return
    try:
        report.status = "Running"
        db.session.commit()
        live = None
        if use_live:
            ext = get_extractor(conn)
            live = ext.get_database_health_snapshot(database_name)
        ctx = build_diagnosis_context(
            export_root, conn.name, database_name, live_snapshot=live
        )
        if len(ctx.strip()) < 80:
            raise ValueError(
                "진단 컨텍스트가 없습니다. 해당 연결에서 DB 추출을 먼저 실행하세요."
            )
        report.report_text = run_agentic_diagnosis(
            ctx, llm_base, llm_model, api_key=api_key
        )
        report.status = "Completed"
        report.message = None
    except Exception as e:
        report.status = "Failed"
        report.message = str(e)
    finally:
        report.completed_at = datetime.utcnow()
        db.session.commit()


def run_diagnosis_queue(
    app,
    export_root: str,
    report_ids: List[int],
    *,
    conn_id: int,
    use_live: bool,
    llm_base: str,
    llm_model: str,
    api_key,
):
    """하나의 백그라운드 스레드에서 report_ids 를 순서대로 처리한다."""

    def worker():
        with app.app_context():
            for rid in report_ids:
                rep = DiagnosisReport.query.get(rid)
                if not rep or not rep.database_name:
                    continue
                execute_diagnosis_report(
                    export_root,
                    rid,
                    conn_id,
                    rep.database_name,
                    use_live,
                    llm_base,
                    llm_model,
                    api_key,
                )

    import threading

    threading.Thread(target=worker, daemon=True).start()
