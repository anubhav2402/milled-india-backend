import os

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

# Use DATABASE_URL if provided (e.g. Postgres on Render), otherwise fall back to local SQLite.
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./emails.db")

if DATABASE_URL.startswith("postgres://"):
    # Render (and some providers) still return postgres:// URLs.
    # SQLAlchemy expects postgresql:// and we explicitly select psycopg (psycopg3) driver.
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg://", 1)

connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}

engine = create_engine(DATABASE_URL, connect_args=connect_args)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

