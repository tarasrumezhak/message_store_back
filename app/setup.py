from app.app import app
from app.app import db

with app.app_context():
    db.create_all()