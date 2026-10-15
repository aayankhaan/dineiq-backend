import os
from dotenv import load_dotenv
from sqlmodel import SQLModel, Session, create_engine
from sqlalchemy import inspect, text

load_dotenv()

engine = create_engine(os.environ["DATABASE_URL"])

def create_db_and_tables():
    SQLModel.metadata.create_all(engine)
    if "restaurant_id" not in {column["name"] for column in inspect(engine).get_columns("user")}:
        with engine.begin() as connection:
            connection.execute(text('ALTER TABLE "user" ADD COLUMN restaurant_id INTEGER'))

def get_session():
    with Session(engine) as session:
        yield session
