# Wuzzuf_Scrapping

A Python web scraper that collects job listings from [Wuzzuf](https://wuzzuf.net) and exports the data to CSV, with Telegram notifications and crash-resume support.
 
## Features
 
- Scrapes job title, company, type, workplace, location, skills and details
- Exports data to CSV (batch files + merged final file)
- Sends each batch directly to Telegram

# Output
 
A CSV file containing all scraped jobs, saved to the `output/` directory.
 
| Column | Description |
|---|---|
| `title` | Job title |
| `company_name` | Company |
| `job_type` | Full-time / Part-time |
| `workplace` | On-site / Remote |
| `location` | City |
| `skills` | Required skills |
| `detail_*` | Extra details (experience, career level, etc.) |
