from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

db = SQLAlchemy()

class DBConnection(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    db_type = db.Column(db.String(20), default='mssql') # 'mssql' or 'mysql'
    name = db.Column(db.String(100), nullable=False)
    host = db.Column(db.String(255), nullable=False)
    port = db.Column(db.Integer, default=1433)
    user = db.Column(db.String(100), nullable=False)
    password = db.Column(db.String(255), nullable=False)

class ExtractionTask(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    conn_name = db.Column(db.String(100))
    status = db.Column(db.String(20), default='Pending') # Pending, Running, Completed, Failed
    progress = db.Column(db.Integer, default=0)
    message = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    completed_at = db.Column(db.DateTime)

class DiagnosisReport(db.Model):
    __tablename__ = 'diagnosis_report'

    id = db.Column(db.Integer, primary_key=True)
    conn_name = db.Column(db.String(100))
    database_name = db.Column(db.String(200))
    report_text = db.Column(db.Text)
    status = db.Column(db.String(20), default='Pending')
    message = db.Column(db.Text)
    llm_model = db.Column(db.String(120))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    completed_at = db.Column(db.DateTime)
