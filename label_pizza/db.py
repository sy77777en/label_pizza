# db.py  – lives next to models.py and app.py
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv(".env")                                 # loads DBURL
engine = create_engine(os.environ["DBURL"], echo=False, future=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
