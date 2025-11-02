"""Scrape Douban Top 250 movies and persist them into a MySQL database."""
from __future__ import annotations

import argparse
import logging
import re
import sys
import time
from dataclasses import dataclass
from typing import Iterable, List, Optional

import requests
from bs4 import BeautifulSoup
import mysql.connector
from mysql.connector import errorcode


BASE_URL = "https://movie.douban.com/top250"
DEFAULT_DB_NAME = "douban_top250"
DEFAULT_DB_USER = "root"
DEFAULT_DB_HOST = "localhost"
DEFAULT_DB_PORT = 3306
PAGE_SIZE = 25
DEFAULT_LIMIT = 250
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


@dataclass
class Movie:
    rank: int
    title: str
    original_title: Optional[str]
    year: Optional[int]
    rating: float
    rating_count: int
    quote: Optional[str]
    poster_url: str
    detail_url: str
    regions: List[str]
    genres: List[str]
    directors: List[str]
    actors: List[str]


def fetch_html(start: int, session: Optional[requests.Session] = None) -> str:
    params = {"start": start, "filter": ""}
    headers = {"User-Agent": USER_AGENT}
    sess = session or requests.Session()
    logging.debug("Fetching %s with params %s", BASE_URL, params)
    response = sess.get(BASE_URL, params=params, headers=headers, timeout=10)
    response.raise_for_status()
    return response.text


def parse_movies(html: str) -> Iterable[Movie]:
    soup = BeautifulSoup(html, "html.parser")
    for item in soup.select(".grid_view li"):
        try:
            yield _parse_movie(item)
        except Exception as exc:  # pragma: no cover - defensive parsing
            logging.warning("Skipping movie due to parse error: %s", exc)


def _parse_movie(item) -> Movie:
    def _required_text(selector: str) -> str:
        tag = item.select_one(selector)
        if not tag:
            raise ValueError(f"Missing required element for selector '{selector}'")
        return tag.get_text(strip=True)

    def _optional_text(selector: str) -> Optional[str]:
        tag = item.select_one(selector)
        return tag.get_text(strip=True) if tag else None

    rank_text = _required_text(".pic em")
    rank = int(rank_text)

    title_tags = item.select(".info .hd .title")
    title = title_tags[0].get_text(strip=True) if title_tags else _required_text(".info .hd a")
    original_title = title_tags[1].get_text(strip=True) if len(title_tags) > 1 else None

    detail_link = item.select_one(".info .hd a")
    if not detail_link or not detail_link.get("href"):
        raise ValueError("Missing detail URL")
    detail_url = detail_link["href"]

    poster_tag = item.select_one(".pic img")
    poster_url = poster_tag.get("src") if poster_tag and poster_tag.get("src") else ""

    rating_text = _required_text(".star .rating_num")
    rating = float(rating_text)

    rating_spans = item.select(".star span")
    if not rating_spans:
        raise ValueError("Missing rating count")
    rating_people_text = rating_spans[-1].get_text(strip=True)
    rating_count_match = re.search(r"(\d+)", rating_people_text.replace(",", ""))
    rating_count = int(rating_count_match.group(1)) if rating_count_match else 0

    quote = _optional_text(".info .bd .inq")

    info_block = item.select_one(".info .bd p")
    directors: List[str] = []
    actors: List[str] = []
    regions: List[str] = []
    genres: List[str] = []
    year: Optional[int] = None

    if info_block:
        info_lines = [line.strip() for line in info_block.get_text("\n").split("\n") if line.strip()]
        if info_lines:
            credits_line = info_lines[0]
            directors, actors = _parse_credits(credits_line)
        if len(info_lines) > 1:
            meta_line = info_lines[1]
            year, regions, genres = _parse_meta(meta_line)

    return Movie(
        rank=rank,
        title=title,
        original_title=original_title,
        year=year,
        rating=rating,
        rating_count=rating_count,
        quote=quote,
        poster_url=poster_url,
        detail_url=detail_url,
        regions=regions,
        genres=genres,
        directors=directors,
        actors=actors,
    )


def _parse_credits(credits_line: str) -> tuple[List[str], List[str]]:
    directors: List[str] = []
    actors: List[str] = []

    if "导演" in credits_line:
        director_part = credits_line.split("导演:", 1)[1]
    else:
        director_part = credits_line

    actor_part = ""
    if "主演:" in director_part:
        director_part, actor_part = director_part.split("主演:", 1)

    directors = [segment.strip() for segment in director_part.split("/") if segment.strip()]
    actors = [segment.strip() for segment in actor_part.split("/") if segment.strip()]
    return directors, actors


def _parse_meta(meta_line: str) -> tuple[Optional[int], List[str], List[str]]:
    parts = [part.strip() for part in meta_line.split("/") if part.strip()]
    year: Optional[int] = None
    regions: List[str] = []
    genres: List[str] = []

    if parts:
        year_match = re.search(r"(\d{4})", parts[0])
        if year_match:
            year = int(year_match.group(1))
    if len(parts) >= 2:
        regions = [region.strip() for region in re.split(r"[, ]+", parts[1]) if region.strip()]
    if len(parts) >= 3:
        genres = [genre.strip() for genre in parts[2].split(" ") if genre.strip()]

    return year, regions, genres


def init_db(connection) -> None:
    cursor = connection.cursor()
    statements = [
        """
        CREATE TABLE IF NOT EXISTS movies (
            id INT AUTO_INCREMENT PRIMARY KEY,
            rank INT NOT NULL,
            title VARCHAR(255) NOT NULL,
            original_title VARCHAR(255),
            year INT,
            rating DECIMAL(3,1) NOT NULL,
            rating_count INT NOT NULL,
            quote TEXT,
            poster_url VARCHAR(500),
            detail_url VARCHAR(255) NOT NULL UNIQUE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS movie_regions (
            movie_id INT NOT NULL,
            region VARCHAR(255) NOT NULL,
            PRIMARY KEY (movie_id, region),
            FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS movie_genres (
            movie_id INT NOT NULL,
            genre VARCHAR(255) NOT NULL,
            PRIMARY KEY (movie_id, genre),
            FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS movie_directors (
            movie_id INT NOT NULL,
            director VARCHAR(255) NOT NULL,
            PRIMARY KEY (movie_id, director),
            FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS movie_actors (
            movie_id INT NOT NULL,
            actor VARCHAR(255) NOT NULL,
            PRIMARY KEY (movie_id, actor),
            FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
    ]

    for statement in statements:
        cursor.execute(statement)

    cursor.close()
    connection.commit()


def store_movies(connection, movies: Iterable[Movie]) -> None:
    cursor = connection.cursor()
    for movie in movies:
        cursor.execute(
            """
            INSERT INTO movies (
                rank, title, original_title, year, rating, rating_count,
                quote, poster_url, detail_url
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                rank=VALUES(rank),
                title=VALUES(title),
                original_title=VALUES(original_title),
                year=VALUES(year),
                rating=VALUES(rating),
                rating_count=VALUES(rating_count),
                quote=VALUES(quote),
                poster_url=VALUES(poster_url)
            """,
            (
                movie.rank,
                movie.title,
                movie.original_title,
                movie.year,
                movie.rating,
                movie.rating_count,
                movie.quote,
                movie.poster_url,
                movie.detail_url,
            ),
        )

        movie_id = cursor.lastrowid
        if not movie_id:
            cursor.execute("SELECT id FROM movies WHERE detail_url = %s", (movie.detail_url,))
            row = cursor.fetchone()
            if not row:
                raise RuntimeError(f"Failed to retrieve movie id for {movie.title}")
            movie_id = row[0]

        _replace_values(cursor, "movie_regions", movie_id, "region", movie.regions)
        _replace_values(cursor, "movie_genres", movie_id, "genre", movie.genres)
        _replace_values(cursor, "movie_directors", movie_id, "director", movie.directors)
        _replace_values(cursor, "movie_actors", movie_id, "actor", movie.actors)

    connection.commit()
    cursor.close()


def _replace_values(cursor, table: str, movie_id: int, column: str, values: Iterable[str]) -> None:
    cursor.execute(f"DELETE FROM {table} WHERE movie_id = %s", (movie_id,))
    values = list(values)
    if not values:
        return
    cursor.executemany(
        f"INSERT INTO {table} (movie_id, {column}) VALUES (%s, %s)",
        [(movie_id, value) for value in values],
    )


def scrape_top_movies(limit: int = DEFAULT_LIMIT, delay: float = 0.5) -> List[Movie]:
    if limit <= 0:
        logging.warning("Requested limit %s is not positive; defaulting to %s", limit, DEFAULT_LIMIT)
        limit = DEFAULT_LIMIT

    limit = min(limit, DEFAULT_LIMIT)

    movies: List[Movie] = []
    with requests.Session() as session:
        for start in range(0, max(limit, PAGE_SIZE), PAGE_SIZE):
            html = fetch_html(start, session=session)
            page_movies = list(parse_movies(html))
            if not page_movies:
                logging.warning("No movies returned for start=%s; stopping early", start)
                break

            movies.extend(page_movies)
            logging.info(
                "Fetched movies %s-%s", start + 1, start + len(page_movies)
            )

            if len(movies) >= limit:
                break

            if delay:
                time.sleep(delay)

    return movies[:limit]


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=DEFAULT_DB_HOST, help="MySQL host name")
    parser.add_argument("--port", type=int, default=DEFAULT_DB_PORT, help="MySQL port")
    parser.add_argument("--user", default=DEFAULT_DB_USER, help="MySQL user")
    parser.add_argument("--password", default="", help="MySQL password")
    parser.add_argument(
        "--database",
        "-d",
        default=DEFAULT_DB_NAME,
        help="MySQL database to store scraped data",
    )
    parser.add_argument(
        "--create-database",
        action="store_true",
        help="Create the target database if it does not already exist",
    )
    parser.add_argument("--delay", type=float, default=0.5, help="Delay in seconds between requests")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Number of top movies to scrape (max 250)")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    args = parser.parse_args(argv)

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))

    movies = scrape_top_movies(limit=args.limit, delay=args.delay)
    logging.info("Parsed %d movies", len(movies))

    try:
        connection = connect_to_database(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database,
            create_database=args.create_database,
        )
    except mysql.connector.Error as exc:
        logging.error("Failed to connect to MySQL: %s", exc)
        return 1

    try:
        init_db(connection)
        store_movies(connection, movies)
    finally:
        connection.close()

    logging.info("Stored movies in MySQL database '%s'", args.database)
    return 0


def connect_to_database(
    *,
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    create_database: bool,
):
    try:
        return mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            autocommit=False,
        )
    except mysql.connector.Error as exc:
        if exc.errno != errorcode.ER_BAD_DB_ERROR or not create_database:
            raise

    admin_connection = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
    )

    admin_cursor = admin_connection.cursor()
    try:
        admin_cursor.execute(
            f"CREATE DATABASE IF NOT EXISTS `{database}` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        )
    finally:
        admin_cursor.close()
        admin_connection.close()

    return mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        autocommit=False,
    )


if __name__ == "__main__":
    sys.exit(main())
