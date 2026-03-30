from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
import os
from models import db, MSSQLConnection
from services.sql_extractor import SQLExtractorService
from services.file_manager import ExportManager

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///connections.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.secret_key = 'mssql_ctrl_secret'

db.init_app(app)

# Ensure export directory exists
EXPORT_ROOT = os.path.join(os.getcwd(), 'exports')
if not os.path.exists(EXPORT_ROOT):
    os.makedirs(EXPORT_ROOT)

@app.route('/')
def index():
    connections = MSSQLConnection.query.all()
    return render_template('index.html', connections=connections)

@app.route('/add_connection', methods=['POST'])
def add_connection():
    name = request.form.get('name')
    host = request.form.get('host')
    port = request.form.get('port', 1433)
    user = request.form.get('user')
    password = request.form.get('password')
    
    new_conn = MSSQLConnection(name=name, host=host, port=int(port), user=user, password=password)
    db.session.add(new_conn)
    db.session.commit()
    flash('Connection added successfully!')
    return redirect(url_for('index'))

@app.route('/export/<int:conn_id>')
def export_database(conn_id):
    conn_info = MSSQLConnection.query.get_or_404(conn_id)
    extractor = SQLExtractorService(conn_info)
    manager = ExportManager(EXPORT_ROOT, conn_info.name)
    
    try:
        # 1. Get all databases
        databases = extractor.get_databases()
        
        for db_name in databases:
            if db_name in ['master', 'tempdb', 'model', 'msdb']: continue
            
            # Create DB folder
            manager.create_db_structure(db_name)
            
            # 2. Extract Tables (DDL + Sample)
            tables = extractor.get_tables(db_name)
            for table in tables:
                ddl = extractor.get_table_ddl(db_name, table)
                sample = extractor.get_table_sample(db_name, table)
                manager.save_table_data(db_name, table, ddl, sample)
            
            # 3. Extract Views
            views = extractor.get_views(db_name)
            for view in views:
                code = extractor.get_object_definition(db_name, view)
                manager.save_object(db_name, 'views', view, code)
            
            # 4. Extract Procedures
            procs = extractor.get_procedures(db_name)
            for proc in procs:
                code = extractor.get_object_definition(db_name, proc)
                manager.save_object(db_name, 'procedures', proc, code)
                
            # 5. Extract Triggers
            triggers = extractor.get_triggers(db_name)
            for trigger in triggers:
                code = extractor.get_object_definition(db_name, trigger)
                manager.save_object(db_name, 'triggers', trigger, code)
                
        flash(f'Export completed for {conn_info.name}!')
    except Exception as e:
        flash(f'Error during export: {str(e)}')
    
    return redirect(url_for('index'))

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(host='0.0.0.0', port=10701, debug=True)
