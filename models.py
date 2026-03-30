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
