def send_csv_to_discord(csv_path):
    """Send filtered job openings to Discord as a formatted message."""
    try:
        df = pd.read_csv(csv_path)
        
        # ✅ Convert date safely without using .dt later
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

        history = load_job_history()
        new_jobs = []
        for _, job in df.iterrows():
            if is_new_job(job, history):
                new_jobs.append(job)
        save_job_history(history)
        
        if not new_jobs:
            message = f"📊 **Job Update** ({datetime.now().strftime('%Y-%m-%d %H:%M')})\n\n"
            message += "No new job openings found for today from target companies.\n"
            message += f"Total jobs checked: {len(df)}\n"
            message += f"Jobs from target companies: {len(df[df['Company'].str.contains('|'.join(map(re.escape, TARGET_COMPANIES)), case=False, na=False)])}\n"
            # ❌ Remove .dt accessor here
        else:
            message = f"🎯 **New Job Openings from Target Companies** ({datetime.now().strftime('%Y-%m-%d %H:%M')})\n\n"
            for job in new_jobs:
                message += f"**Company:** {job['Company']}\n"
                message += f"**Position:** {job['Position Title']}\n"
                # ❌ Date removed
                message += f"**Apply:** {job['Apply']}\n"
                message += "-------------------\n\n"
            message += f"\nTotal new jobs found: {len(new_jobs)}"
        
        logger.info(f"Sending the following message to Discord:\n{message}")
        payload = {
            "content": message,
            "username": "Job Scraper Bot",
            "avatar_url": "https://i.imgur.com/4M34hi2.png"
        }
        response = requests.post(WEBHOOK_URL, json=payload)
        if response.status_code == 200:
            logger.info("Successfully sent job openings to Discord")
            return True
        else:
            logger.error(f"Failed to send to Discord. Status code: {response.status_code}")
            logger.error(f"Response content: {response.text}")
            return False
    except Exception as e:
        logger.error(f"Error sending to Discord: {e}")
        return False
