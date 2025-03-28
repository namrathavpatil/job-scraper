import sqlite3
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

class JobDatabase:
    def __init__(self, db_path="job_data/jobs.db"):
        self.db_path = db_path
        self._ensure_db_exists()
    
    def _ensure_db_exists(self):
        """Ensure the database and tables exist."""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Create jobs table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company TEXT NOT NULL,
                    position_title TEXT NOT NULL,
                    apply_url TEXT,
                    date_posted TIMESTAMP,
                    date_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(company, position_title)
                )
            ''')
            
            conn.commit()
            logger.info("Database initialized successfully")
    
    def is_job_seen(self, company, position_title):
        """Check if a job has been seen before."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id FROM jobs 
                WHERE company = ? AND position_title = ?
            ''', (company, position_title))
            return cursor.fetchone() is not None
    
    def mark_job_seen(self, company, position_title, apply_url=None, date_posted=None):
        """Mark a job as seen."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR IGNORE INTO jobs 
                    (company, position_title, apply_url, date_posted)
                    VALUES (?, ?, ?, ?)
                ''', (company, position_title, apply_url, date_posted))
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error marking job as seen: {e}")
            return False
    
    def get_jobs_seen_today(self):
        """Get all jobs seen today."""
        today = datetime.now().date()
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT company, position_title, apply_url, date_posted
                FROM jobs
                WHERE date(date_seen) = date('now')
            ''')
            return cursor.fetchall()
    
    def cleanup_old_jobs(self, days=30):
        """Remove jobs older than specified days."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                DELETE FROM jobs
                WHERE date(date_seen) < date('now', ?)
            ''', (f'-{days} days',))
            deleted = cursor.rowcount
            conn.commit()
            logger.info(f"Cleaned up {deleted} old jobs")
            return deleted 