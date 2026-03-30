from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
import os, threading
from datetime import datetime
from models import db, DBConnection, ExtractionTask
from services.sql_extractor import get_extractor
from services.file_manager import ExportManager

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///connections.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.secret_key = 'db_ctrl_secret'

db.init_app(app)
EXPORT_ROOT = os.path.join(os.getcwd(), 'exports')
if not os.path.exists(EXPORT_ROOT): os.makedirs(EXPORT_ROOT)

@app.route('/')
def index():
    conns = DBConnection.query.all()
    tasks = ExtractionTask.query.order_by(ExtractionTask.created_at.desc()).limit(10).all()
    return render_template('index.html', connections=conns, tasks=tasks)

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
                    # Tables
                    for t in ext.get_tables(db_name):
                        mgr.save_table_data(db_name, t, ext.get_table_ddl(db_name, t), ext.get_table_sample(db_name, t))
                    # Others
                    for ot, meth in [('views','get_views'),('procedures','get_procedures'),('triggers','get_triggers')]:
                        for o in getattr(ext, meth)(db_name):
                            mgr.save_object(db_name, ot, o, ext.get_object_definition(db_name, o))
                    
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

if __name__ == '__main__':
    with app.app_context(): db.create_all()
    app.run(host='0.0.0.0', port=10701, debug=True)
