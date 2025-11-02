"""Scrape Douban Top 100 movies and persist them into a SQLite database."""
from __future__ import annotations

import argparse
import logging
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional

import requests
from bs4 import BeautifulSoup


BASE_URL = "https://movie.douban.com/top250"
DEFAULT_DB_PATH = Path("data/douban_top100.sqlite3")
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


def init_db(connection: sqlite3.Connection) -> None:
    connection.execute("PRAGMA foreign_keys = ON")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS movies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rank INTEGER NOT NULL,
            title TEXT NOT NULL,
            original_title TEXT,
            year INTEGER,
            rating REAL NOT NULL,
            rating_count INTEGER NOT NULL,
            quote TEXT,
            poster_url TEXT,
            detail_url TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS movie_regions (
            movie_id INTEGER NOT NULL,
            region TEXT NOT NULL,
            PRIMARY KEY (movie_id, region),
            FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS movie_genres (
            movie_id INTEGER NOT NULL,
            genre TEXT NOT NULL,
            PRIMARY KEY (movie_id, genre),
            FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS movie_directors (
            movie_id INTEGER NOT NULL,
            director TEXT NOT NULL,
            PRIMARY KEY (movie_id, director),
            FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS movie_actors (
            movie_id INTEGER NOT NULL,
            actor TEXT NOT NULL,
            PRIMARY KEY (movie_id, actor),
            FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE
        );
        """
    )


def store_movies(connection: sqlite3.Connection, movies: Iterable[Movie]) -> None:
    cursor = connection.cursor()
    for movie in movies:
        cursor.execute(
            """
            INSERT INTO movies (
                rank, title, original_title, year, rating, rating_count,
                quote, poster_url, detail_url
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(detail_url) DO UPDATE SET
                rank=excluded.rank,
                title=excluded.title,
                original_title=excluded.original_title,
                year=excluded.year,
                rating=excluded.rating,
                rating_count=excluded.rating_count,
                quote=excluded.quote,
                poster_url=excluded.poster_url
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
            cursor.execute("SELECT id FROM movies WHERE detail_url = ?", (movie.detail_url,))
            row = cursor.fetchone()
            if not row:
                raise RuntimeError(f"Failed to retrieve movie id for {movie.title}")
            movie_id = row[0]

        _replace_values(cursor, "movie_regions", movie_id, "region", movie.regions)
        _replace_values(cursor, "movie_genres", movie_id, "genre", movie.genres)
        _replace_values(cursor, "movie_directors", movie_id, "director", movie.directors)
        _replace_values(cursor, "movie_actors", movie_id, "actor", movie.actors)

    connection.commit()


def _replace_values(cursor: sqlite3.Cursor, table: str, movie_id: int, column: str, values: Iterable[str]) -> None:
    cursor.execute(f"DELETE FROM {table} WHERE movie_id = ?", (movie_id,))
    cursor.executemany(
        f"INSERT INTO {table} (movie_id, {column}) VALUES (?, ?)",
        ((movie_id, value) for value in values),
    )


def scrape_top100(delay: float = 0.5) -> List[Movie]:
    movies: List[Movie] = []
    with requests.Session() as session:
        for start in range(0, 100, 25):
            html = fetch_html(start, session=session)
            movies.extend(parse_movies(html))
            logging.info("Fetched movies %s-%s", start + 1, start + 25)
            if delay:
                time.sleep(delay)
    return movies[:100]


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", "-d", type=Path, default=DEFAULT_DB_PATH, help="Path to the SQLite database file")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay in seconds between requests")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    args = parser.parse_args(argv)

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))

    if args.database.parent and not args.database.parent.exists():
        args.database.parent.mkdir(parents=True, exist_ok=True)

    movies = scrape_top100(delay=args.delay)
    logging.info("Parsed %d movies", len(movies))

    with sqlite3.connect(args.database) as connection:
        init_db(connection)
        store_movies(connection, movies)

    logging.info("Stored movies in %s", args.database)
    return 0


if __name__ == "__main__":
    sys.exit(main())
