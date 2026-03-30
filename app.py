from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
import os
import threading
from datetime import datetime
from sqlalchemy import inspect as sa_inspect, text

from models import db, DBConnection, ExtractionTask, DiagnosisReport
from services.sql_extractor import get_extractor
from services.file_manager import ExportManager
from services.diagnosis_worker import execute_diagnosis_report, run_diagnosis_queue
from services.ollama_control import (
    v1_base_to_origin,
    ollama_ping,
    ollama_version,
    ollama_list_models,
    try_start_ollama_server,
    start_pull_in_thread,
    start_pull_sequence_in_thread,
    pull_status,
    suggested_models_catalog,
)


def _app_base_dir() -> str:
    return os.path.dirname(os.path.abspath(__file__))


def _sqlite_uri(abs_path: str) -> str:
    return "sqlite:///" + os.path.abspath(abs_path).replace("\\", "/")


def _exports_tree_has_data(exports_root: str) -> bool:
    """연결명/DB명/tables 구조가 하나라도 있으면 True (실추출 데이터 존재)."""
    if not os.path.isdir(exports_root):
        return False
    try:
        for conn_dir in os.listdir(exports_root):
            cp = os.path.join(exports_root, conn_dir)
            if not os.path.isdir(cp):
                continue
            for dbname in os.listdir(cp):
                tables = os.path.join(cp, dbname, "tables")
                if os.path.isdir(tables):
                    return True
    except OSError:
        pass
    return False


def _resolve_export_root() -> str:
    """
    프로덕션에서 cwd가 달라져 빈 exports 를 보는 문제 방지.
    우선순위: MSSQLCTRL_EXPORT_ROOT → app.py 옆 exports(데이터 있으면) → cwd/exports(데이터 있으면) → app.py 옆 exports(신규).
    """
    if os.environ.get("MSSQLCTRL_EXPORT_ROOT"):
        return os.path.abspath(os.environ["MSSQLCTRL_EXPORT_ROOT"])
    app_dir = _app_base_dir()
    next_app = os.path.join(app_dir, "exports")
    cwd_exp = os.path.join(os.getcwd(), "exports")
    if _exports_tree_has_data(next_app):
        return next_app
    if _exports_tree_has_data(cwd_exp):
        return cwd_exp
    return next_app


def _resolve_sqlite_uri() -> str:
    if os.environ.get("MSSQLCTRL_DATABASE_URL"):
        return os.environ["MSSQLCTRL_DATABASE_URL"]
    app_dir = _app_base_dir()
    db_app = os.path.join(app_dir, "connections.db")
    db_cwd = os.path.join(os.getcwd(), "connections.db")
    if os.path.isfile(db_app):
        return _sqlite_uri(db_app)
    if os.path.isfile(db_cwd):
        return _sqlite_uri(db_cwd)
    return _sqlite_uri(db_app)


app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = _resolve_sqlite_uri()
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.secret_key = "db_ctrl_secret"

db.init_app(app)
EXPORT_ROOT = _resolve_export_root()
os.makedirs(EXPORT_ROOT, exist_ok=True)


def ensure_diagnosis_schema():
    if "sqlite" not in app.config["SQLALCHEMY_DATABASE_URI"]:
        return
    insp = sa_inspect(db.engine)
    if not insp.has_table("diagnosis_report"):
        return
    with db.engine.connect() as conn:
        cols = {row[1] for row in conn.execute(text("PRAGMA table_info(diagnosis_report)"))}
        for stmt in (
            "ALTER TABLE diagnosis_report ADD COLUMN database_name VARCHAR(200)",
            "ALTER TABLE diagnosis_report ADD COLUMN status VARCHAR(20)",
            "ALTER TABLE diagnosis_report ADD COLUMN message TEXT",
            "ALTER TABLE diagnosis_report ADD COLUMN llm_model VARCHAR(120)",
            "ALTER TABLE diagnosis_report ADD COLUMN completed_at DATETIME",
        ):
            col = stmt.split("ADD COLUMN ")[1].split()[0]
            if col not in cols:
                conn.execute(text(stmt))
                conn.commit()


with app.app_context():
    db.create_all()
    ensure_diagnosis_schema()


@app.route('/')
def index():
    conns = DBConnection.query.all()
    return render_template('index.html', connections=conns)


@app.route('/agent')
def agent_page():
    return render_template('agent.html')

@app.route('/add_connection', methods=['POST'])
def add_connection():
    new_conn = DBConnection(
        db_type=request.form.get('db_type'),
        name=request.form.get('name'),
        host=request.form.get('host'),
        port=int(request.form.get('port', 1433)),
        user=request.form.get('user'),
        password=request.form.get('password')
    )
    db.session.add(new_conn)
    db.session.commit()
    return redirect(url_for('index'))

@app.route('/get_databases/<int:conn_id>')
def get_databases(conn_id):
    c = DBConnection.query.get_or_404(conn_id)
    try:
        return jsonify({'success': True, 'databases': get_extractor(c).get_databases()})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

def run_extraction(task_id, selections):
    with app.app_context():
        task = ExtractionTask.query.get(task_id)
        task.status = 'Running'
        db.session.commit()
        
        try:
            total_steps = sum(len(s.get('databases', [])) or 1 for s in selections)
            current_step = 0
            
            for sel in selections:
                c = DBConnection.query.get(sel['conn_id'])
                ext = get_extractor(c)
                mgr = ExportManager(EXPORT_ROOT, c.name)
                dbs = sel.get('databases') or ext.get_databases()
                
                for db_name in dbs:
                    mgr.create_db_structure(db_name)
                    expected_files = set()
                    # Tables
                    for t in ext.get_tables(db_name):
                        expected_files.add(f"tables/{t}_schema.sql")
                        expected_files.add(f"tables/{t}_sample.json")
                        mgr.save_table_data(db_name, t, ext.get_table_ddl(db_name, t), ext.get_table_sample(db_name, t))
                    # Others
                    for ot, meth in [('views','get_views'),('procedures','get_procedures'),('triggers','get_triggers')]:
                        for o in getattr(ext, meth)(db_name):
                            expected_files.add(f"{ot}/{o}.sql")
                            mgr.save_object(db_name, ot, o, ext.get_object_definition(db_name, o))
                    try:
                        mgr.save_db_metadata(db_name, ext.get_database_health_snapshot(db_name))
                    except Exception as meta_err:
                        mgr.save_db_metadata(db_name, {'database': db_name, 'error': str(meta_err)})
                    expected_files.add("db_metadata.json")
                    mgr.prune_db_export(db_name, expected_files)

                    current_step += 1
                    task.progress = int((current_step / total_steps) * 100)
                    db.session.commit()

            task.status = 'Completed'
            task.progress = 100
        except Exception as e:
            task.status = 'Failed'
            task.message = str(e)
        finally:
            task.completed_at = datetime.utcnow()
            db.session.commit()

@app.route('/batch_export', methods=['POST'])
def batch_export():
    selections = request.json.get('selections', [])
    task = ExtractionTask(conn_name=", ".join([DBConnection.query.get(s['conn_id']).name for s in selections]))
    db.session.add(task)
    db.session.commit()
    
    threading.Thread(target=run_extraction, args=(task.id, selections)).start()
    return jsonify({'success': True, 'task_id': task.id})

@app.route('/tasks')
def get_tasks():
    tasks = ExtractionTask.query.order_by(ExtractionTask.created_at.desc()).all()
    return jsonify([{
        'id': t.id, 'conn_name': t.conn_name, 'status': t.status, 
        'progress': t.progress, 'message': t.message, 
        'created_at': t.created_at.strftime('%H:%M:%S')
    } for t in tasks])


def _llm_v1_base_from_request():
    if request.method == "GET":
        raw = request.args.get("llm_base_url")
    else:
        raw = (request.json or {}).get("llm_base_url")
    return (raw or os.getenv("LLM_BASE_URL") or "http://127.0.0.1:11434/v1").strip().rstrip(
        "/"
    )


@app.route("/ollama/status")
def ollama_status():
    v1 = _llm_v1_base_from_request()
    origin = v1_base_to_origin(v1)
    ok, err = ollama_ping(origin)
    ver = ollama_version(origin) if ok else None
    return jsonify(
        {
            "ok": ok,
            "llm_base_url": v1,
            "ollama_origin": origin,
            "error": err,
            "version": ver,
        }
    )


@app.route("/ollama/models")
def ollama_models():
    v1 = _llm_v1_base_from_request()
    origin = v1_base_to_origin(v1)
    try:
        models = ollama_list_models(origin)
        return jsonify({"success": True, "models": models, "ollama_origin": origin})
    except Exception as e:
        return jsonify(
            {
                "success": False,
                "error": str(e),
                "models": [],
                "ollama_origin": origin,
            }
        ), 502


@app.route("/ollama/start", methods=["POST"])
def ollama_start():
    v1 = _llm_v1_base_from_request()
    origin = v1_base_to_origin(v1)
    out = try_start_ollama_server(origin)
    out["ollama_origin"] = origin
    out["llm_base_url"] = v1
    return jsonify(out)


@app.route("/ollama/catalog")
def ollama_catalog():
    return jsonify({"models": suggested_models_catalog()})


@app.route("/ollama/pull", methods=["POST"])
def ollama_pull():
    data = request.json or {}
    models = data.get("models")
    if isinstance(models, list) and len(models) > 0:
        ok, err = start_pull_sequence_in_thread([str(m) for m in models if m])
    else:
        ok, err = start_pull_in_thread(data.get("name") or data.get("model") or "")
    if not ok:
        return jsonify({"success": False, "error": err}), 400
    return jsonify({"success": True})


@app.route("/ollama/pull/status")
def ollama_pull_status():
    return jsonify(pull_status())


@app.route("/export_catalog")
def export_catalog():
    out = []
    for c in DBConnection.query.all():
        slug = c.name.replace(" ", "_")
        root = os.path.join(EXPORT_ROOT, slug)
        databases = []
        if os.path.isdir(root):
            for name in sorted(os.listdir(root)):
                p = os.path.join(root, name)
                if os.path.isdir(p) and os.path.isdir(os.path.join(p, "tables")):
                    databases.append({
                        "name": name,
                        "has_metadata": os.path.isfile(os.path.join(p, "db_metadata.json")),
                    })
        out.append({"conn_id": c.id, "conn_name": c.name, "databases": databases})
    return jsonify({
        "export_root": EXPORT_ROOT,
        "connections": out,
    })


def run_diagnosis_job(report_id, conn_id, database_name, use_live, llm_base, llm_model, api_key):
    with app.app_context():
        execute_diagnosis_report(
            EXPORT_ROOT,
            report_id,
            conn_id,
            database_name,
            use_live,
            llm_base,
            llm_model,
            api_key,
        )


@app.route('/diagnosis/start', methods=['POST'])
def diagnosis_start():
    data = request.json or {}
    conn_id = data.get("conn_id")
    database_name = data.get("database")
    if conn_id is None or not database_name:
        return jsonify({"success": False, "error": "conn_id 와 database 가 필요합니다."}), 400
    c = DBConnection.query.get_or_404(conn_id)
    base = (data.get("llm_base_url") or os.getenv("LLM_BASE_URL") or "http://127.0.0.1:11434/v1").rstrip("/")
    model = data.get("model") or os.getenv("LLM_MODEL") or "qwen2.5:latest"
    api_key = data.get("api_key") or os.getenv("LLM_API_KEY") or None
    use_live = bool(data.get("use_live"))
    report = DiagnosisReport(
        conn_name=c.name,
        database_name=database_name,
        status="Pending",
        llm_model=model,
    )
    db.session.add(report)
    db.session.commit()
    threading.Thread(
        target=run_diagnosis_job,
        args=(report.id, conn_id, database_name, use_live, base, model, api_key),
    ).start()
    return jsonify({"success": True, "report_id": report.id})


@app.route("/diagnosis/batch", methods=["POST"])
def diagnosis_batch():
    data = request.json or {}
    conn_id = data.get("conn_id")
    databases = data.get("databases")
    if conn_id is None or not isinstance(databases, list) or not databases:
        return jsonify(
            {"success": False, "error": "conn_id 와 비어 있지 않은 databases 배열이 필요합니다."}
        ), 400
    c = DBConnection.query.get_or_404(conn_id)
    base = (data.get("llm_base_url") or os.getenv("LLM_BASE_URL") or "http://127.0.0.1:11434/v1").rstrip("/")
    model = data.get("model") or os.getenv("LLM_MODEL") or "qwen2.5:latest"
    api_key = data.get("api_key") or os.getenv("LLM_API_KEY") or None
    use_live = bool(data.get("use_live"))
    report_ids = []
    for dbname in databases:
        name = (dbname or "").strip()
        if not name:
            continue
        rep = DiagnosisReport(
            conn_name=c.name,
            database_name=name,
            status="Pending",
            llm_model=model,
        )
        db.session.add(rep)
        db.session.flush()
        report_ids.append(rep.id)
    db.session.commit()
    if not report_ids:
        return jsonify({"success": False, "error": "유효한 DB 이름이 없습니다."}), 400
    run_diagnosis_queue(
        app,
        EXPORT_ROOT,
        report_ids,
        conn_id=int(conn_id),
        use_live=use_live,
        llm_base=base,
        llm_model=model,
        api_key=api_key,
    )
    return jsonify({"success": True, "report_ids": report_ids, "count": len(report_ids)})


@app.route('/diagnosis/reports')
def diagnosis_reports():
    rows = DiagnosisReport.query.order_by(DiagnosisReport.created_at.desc()).limit(30).all()
    return jsonify([
        {
            "id": r.id,
            "conn_name": r.conn_name,
            "database_name": r.database_name,
            "status": r.status,
            "message": r.message,
            "llm_model": r.llm_model,
            "created_at": r.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "completed_at": r.completed_at.strftime("%Y-%m-%d %H:%M:%S") if r.completed_at else None,
            "preview": (r.report_text or "")[:280],
        }
        for r in rows
    ])


@app.route('/diagnosis/report/<int:report_id>')
def diagnosis_report_one(report_id):
    r = DiagnosisReport.query.get_or_404(report_id)
    return jsonify({
        "id": r.id,
        "conn_name": r.conn_name,
        "database_name": r.database_name,
        "status": r.status,
        "message": r.message,
        "llm_model": r.llm_model,
        "report_text": r.report_text or "",
        "created_at": r.created_at.isoformat(),
        "completed_at": r.completed_at.isoformat() if r.completed_at else None,
    })


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10701, debug=True)
