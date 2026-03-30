from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

class MSSQLConnection(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    host = db.Column(db.String(255), nullable=False)
    port = db.Column(db.Integer, default=1433)
    user = db.Column(db.String(100), nullable=False)
    password = db.Column(db.String(255), nullable=False)

    def __repr__(self):
        return f'<MSSQLConnection {self.name}>'
