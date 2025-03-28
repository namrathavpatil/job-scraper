# Job Scraper Bot

A Python-based job scraping bot that monitors job postings from Airtable and sends notifications to Discord for specific target companies.

## Features

- Scrapes job postings from Airtable
- Filters jobs by target companies (Google, Microsoft, Amazon, Meta, Apple, TikTok, Draper)
- Sends notifications to Discord for new job openings
- Maintains job history to avoid duplicate notifications
- Runs continuously with hourly checks
- Saves filtered jobs to Excel for easy tracking

## Setup

1. Clone the repository:
```bash
git clone https://github.com/yourusername/job-scraper.git
cd job-scraper
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
- `WEBHOOK_URL`: Your Discord webhook URL
- `AIRTABLE_URL`: Your Airtable URL

4. Run the script:
```bash
python import_requests.py
```

## Deployment

This project is configured for deployment on Render. To deploy:

1. Push your code to GitHub
2. Connect your repository to Render
3. Set up the environment variables in Render's dashboard
4. Deploy!

## Configuration

You can modify the target companies in `import_requests.py`:
```python
TARGET_COMPANIES = ["Google", "Microsoft", "Amazon", "Meta", "Apple", "TikTok", "Draper"]
```

## License

MIT License 