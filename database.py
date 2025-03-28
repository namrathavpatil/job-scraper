import logging
from datetime import datetime
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import pandas as pd
import os

logger = logging.getLogger(__name__)

class JobDatabase:
    def __init__(self):
        # Get MongoDB URI from environment variable
        uri = os.getenv('MONGODB_URI')
        if not uri:
            raise ValueError("MONGODB_URI environment variable is not set")
        
        try:
            # Create a new client and connect to the server
            self.client = MongoClient(uri, server_api=ServerApi('1'))
            # Send a ping to confirm a successful connection
            self.client.admin.command('ping')
            logger.info("Successfully connected to MongoDB!")
            
            # Get the database and collection
            self.db = self.client.job_scraper
            self.jobs = self.db.jobs
            
            # Create unique index on company and position_title
            self.jobs.create_index([("company", 1), ("position_title", 1)], unique=True)
            
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
