from fastapi import FastAPI
from app.database import create_db_and_tables
from app.models import User

app = FastAPI(title="DineIQ API", version="1.0.0")

create_db_and_tables()

@app.get("/")
def root():
    return {"message": "DineIQ API is running"}

@app.get("/health")
def check_health():
    return {"message": "api is running healthy!"}
