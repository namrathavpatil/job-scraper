import logging
from datetime import datetime
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import pandas as pd
import os

logger = logging.getLogger(__name__)

class JobDatabase:
    def __init__(self):
        # MongoDB connection settings
        uri = "mongodb+srv://namratha1082000:KwSpFDRErT7tydOT@cluster0.kuf6ahl.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
        DATABASE_NAME = "jobs"
        COLLECTION_NAME = "jobsearch"
        
        try:
            # Create a new client and connect to the server
            self.client = MongoClient(uri, server_api=ServerApi('1'))
            
            # Send a ping to confirm a successful connection
            self.client.admin.command('ping')
            logger.info("Successfully connected to MongoDB!")
            
            # Get the database and collection
            self.db = self.client[DATABASE_NAME]
            self.jobs = self.db[COLLECTION_NAME]
            
            # Create unique index on company and position_title
            self.jobs.create_index([("company", 1), ("position_title", 1)], unique=True)
            
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    def is_job_seen(self, company, position_title):
        """Check if a job has been seen before."""
        try:
            return self.jobs.find_one({
                "company": company,
                "position_title": position_title
            }) is not None
        except Exception as e:
            logger.error(f"Error checking if job is seen: {e}")
            return False
    
    def mark_job_seen(self, company, position_title, apply_url=None, date_posted=None):
        """Mark a job as seen."""
        try:
            # Convert Timestamp to ISO format string if it's a Timestamp object
            if isinstance(date_posted, pd.Timestamp):
                date_posted = date_posted.isoformat()
            elif date_posted is not None:
                date_posted = str(date_posted)
            
            job_data = {
                "company": company,
                "position_title": position_title,
                "apply_url": apply_url,
                "date_posted": date_posted,
                "date_seen": datetime.utcnow()
            }
            
            self.jobs.update_one(
                {"company": company, "position_title": position_title},
                {"$set": job_data},
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Error marking job as seen: {e}")
            return False
    
    def get_jobs_seen_today(self):
        """Get all jobs seen today."""
        try:
            today = datetime.utcnow().date()
            return list(self.jobs.find({
                "date_seen": {
                    "$gte": datetime.combine(today, datetime.min.time()),
                    "$lt": datetime.combine(today, datetime.max.time())
                }
            }))
        except Exception as e:
            logger.error(f"Error getting jobs seen today: {e}")
            return []
    
    def cleanup_old_jobs(self, days=30):
        """Remove jobs older than specified days."""
        try:
            cutoff_date = datetime.utcnow() - pd.Timedelta(days=days)
            result = self.jobs.delete_many({
                "date_seen": {"$lt": cutoff_date}
            })
            deleted = result.deleted_count
            logger.info(f"Cleaned up {deleted} old jobs")
            return deleted
        except Exception as e:
            logger.error(f"Error cleaning up old jobs: {e}")
            return 0 
