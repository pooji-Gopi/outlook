import os
from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler

app = FastAPI()

# Define SQLAlchemy model
Base = declarative_base()

class DwhUsers(Base):
    __tablename__ = 'dwh_users'

    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(String)
    full_name = Column(String)
    email = Column(String)
    status = Column(Integer)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)

class DwhAdmin(Base):
    __tablename__ = 'dwh_admin'

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(100))
    password = Column(String(50))
    email = Column(String(30))

class ScheduleStatus(Base):
    __tablename__ = 'schedule_status'

    id = Column(Integer, primary_key=True, autoincrement=True)
    table_name = Column(String)
    last_executed = Column(DateTime)
    success = Column(Boolean)

# MySQL database connection
URL_DATABASE = 'mysql+pymysql://root:Sql#24,my()@localhost:3306/cn2taskbench_analytics'
engine = create_engine(URL_DATABASE)

# Create session
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Function to be scheduled
def scheduled_task():
    try:
        # Your task logic here
        # For example, let's just print something
        print("Scheduled task executed successfully.")
        with SessionLocal() as session:
            # Update schedule status for dwh_users
            status_record = session.query(ScheduleStatus).filter_by(table_name='dwh_users').first()
            if status_record:
                status_record.last_executed = datetime.now()
                status_record.success = True
            else:
                new_status_record = ScheduleStatus(table_name='dwh_users', last_executed=datetime.now(), success=True)
                session.add(new_status_record)
            # Update schedule status for dwh_admin
            admin_status_record = session.query(ScheduleStatus).filter_by(table_name='dwh_admin').first()
            if admin_status_record:
                admin_status_record.last_executed = datetime.now()
                admin_status_record.success = True
            else:
                new_admin_status_record = ScheduleStatus(table_name='dwh_admin', last_executed=datetime.now(), success=True)
                session.add(new_admin_status_record)
            session.commit()
    except Exception as e:
        print(f"Error in scheduled task: {str(e)}")
        with SessionLocal() as session:
            # Update schedule status if task fails for dwh_users
            status_record = session.query(ScheduleStatus).filter_by(table_name='dwh_users').first()
            if status_record:
                status_record.last_executed = datetime.now()
                status_record.success = False
            else:
                new_status_record = ScheduleStatus(table_name='dwh_users', last_executed=datetime.now(), success=False)
                session.add(new_status_record)
            # Update schedule status if task fails for dwh_admin
            admin_status_record = session.query(ScheduleStatus).filter_by(table_name='dwh_admin').first()
            if admin_status_record:
                admin_status_record.last_executed = datetime.now()
                admin_status_record.success = False
            else:
                new_admin_status_record = ScheduleStatus(table_name='dwh_admin', last_executed=datetime.now(), success=False)
                session.add(new_admin_status_record)
            session.commit()

# Scheduler setup
scheduler = BackgroundScheduler()
scheduler.add_job(scheduled_task, 'cron', hour=15, minute=15)  # Schedule daily at 3:15 PM
scheduler.start()

@app.post("/upload/")
async def upload_csv(file_name: str):
    file_path = os.path.join("D:\\major project\\csv_file", file_name)
    
    if not os.path.exists(file_path):
        return {"error": "File not found."}
    
    try:
        # Read CSV file using pandas
        df = pd.read_csv(file_path)
        
        # Process CSV data - Example: Print first 5 rows
        result = df.head(6).to_dict()

        # Check if dwh_users data is provided in the CSV
        if {'uuid', 'full_name', 'email', 'status'}.issubset(df.columns):
            # Insert data into database for dwh_users
            with SessionLocal() as session:
                for index, row in df.iterrows():
                    db_data = DwhUsers(
                        uuid=row['uuid'],
                        full_name=row['full_name'],
                        email=row['email'],
                        status=row['status'],
                        created_at=datetime.now(),
                        updated_at=datetime.now()
                    )
                    session.add(db_data)
                session.commit()

        # Check if dwh_admin data is provided in the CSV
        if {'username', 'password', 'email'}.issubset(df.columns):
            # Insert data into database for dwh_admin
            with SessionLocal() as session:
                for index, row in df.iterrows():
                    admin_data = DwhAdmin(
                        username=row['username'],
                        password=row['password'],
                        email=row['email']
                    )
                    session.add(admin_data)
                session.commit()

        return {"success": True, "data": result}
    except Exception as e:
        return {"error": str(e)}
