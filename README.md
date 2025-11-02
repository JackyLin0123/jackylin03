# Douban Top 250 Scraper

This project provides a command-line tool for collecting the latest **Top 250** movie listings from [Douban Movies](https://movie.douban.com/top250) and storing the results in a MySQL database. The scraper is designed for data analysis or visualization backends that expect a relational schema tailored to the movie metadata exposed on Douban.

## Features

- Pagination-aware crawler that requests up to 250 ranked movies from Douban.
- Robust HTML parsing with defensive validation to ensure reliable metadata extraction.
- MySQL persistence layer that can bootstrap the database schema on first run.
- Configurable CLI flags for credentials, connection handling, and limiting the number of scraped entries.

## Requirements

- Python 3.9+
- A reachable MySQL 8.x or compatible server instance.
- Network access to `movie.douban.com`.

Install Python dependencies with:

```bash
pip install -r requirements.txt
```

## Database Schema

The scraper operates on a single `movies` table created in the target database when the `--init-db` flag is provided:

| Column         | Type                | Description                                  |
| -------------- | ------------------- | -------------------------------------------- |
| `id`           | `INT` (auto-increment) | Internal surrogate key.                   |
| `douban_id`    | `VARCHAR(32)`       | Unique Douban movie identifier.              |
| `rank`         | `INT`               | Position within the Top 250 list.            |
| `title`        | `VARCHAR(255)`      | Primary movie title.                         |
| `original_title` | `VARCHAR(255)`    | Original-language title when available.      |
| `year`         | `INT`               | Release year.                                |
| `region`       | `VARCHAR(255)`      | Country or region of origin.                 |
| `genre`        | `VARCHAR(255)`      | Primary genres as a comma-separated string.  |
| `rating`       | `DECIMAL(3,1)`      | Douban average rating.                       |
| `num_reviews`  | `INT`               | Number of reviews counted by Douban.         |
| `quote`        | `TEXT`              | Highlighted quote shown in the listing.      |
| `detail_url`   | `VARCHAR(255)`      | Direct link to the movie detail page.        |
| `poster_url`   | `VARCHAR(255)`      | Poster image URL from the list page.         |
| `created_at`   | `TIMESTAMP`         | Insertion timestamp (defaults to `CURRENT_TIMESTAMP`). |

## Usage

1. **Provision a MySQL database** and create a user account with privileges to create tables and insert rows.
2. **Run the scraper** with the appropriate connection details:

   ```bash
   python -m src.douban_top100 \
       --host 127.0.0.1 \
       --port 3306 \
       --user douban \
       --password your-secret \
       --database douban_movies \
       --init-db
   ```

   The script will crawl the full Top 250 list and populate the `movies` table. If you only want a subset, add `--limit 50` (or any value between 1 and 250).

3. **Reuse the populated data** for dashboards or analytical tools that expect a structured relational dataset.

## Operational Notes

- Respect Douban's terms of service and avoid aggressive scraping patterns.
- The scraper introduces a small delay between requests to reduce load on the site.
- If the script encounters network errors, it will retry the affected page before failing.
- Use MySQL connection parameters that align with your hosting environment (socket, SSL, or cloud-hosted endpoints).

## Troubleshooting

| Symptom | Resolution |
| ------- | ---------- |
| `Access denied for user` | Verify the username, password, and host permissions in MySQL. |
| `Can't connect to MySQL server` | Confirm the server is reachable and that firewalls allow traffic on the configured port. |
| `Douban parsing error` | Re-run with `--log-level DEBUG` to inspect HTML differences or temporary blocking. |
| Unexpected duplicate entries | Ensure the `movies` table is empty or truncate it before re-running with a different `--limit`. |

For more CLI options, run:

```bash
python -m src.douban_top100 --help
```
