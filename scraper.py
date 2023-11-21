import os
import re
import sys
import time
import warnings

from duckdb_engine import DuckDBEngineWarning
from loguru import logger
import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine


def create_logger(
    log_name="main",
    log_level="INFO",
):
    fmt = (
        "[<g>{time:YYYY-MM-DD HH:mm:ss.SSSZ}</g> :: <c>{level}</c> ::"
        + " <e>{process.id}</e> :: <y>{process.name}</y>] {message}"
    )

    logger.remove()
    logger.add(
        sys.stdout,
        level=log_level,
        backtrace=True,
        diagnose=True,
        format=fmt,
        enqueue=True,
    )
    return logger


class Scraper:
    poem_root_url = "https://www.poesie-francaise.fr/poemes-"
    poet_root_url = "https://www.poesie-francaise.fr/poemes-auteurs/"

    def __init__(self, duckdb_file_path=None, log_level="INFO"):
        self.logger = create_logger(log_level=log_level)
        self.logger.info("**** poesie francaise scraper init ****")
        warnings.filterwarnings("ignore", category=DuckDBEngineWarning)
        if duckdb_file_path is None:
            self.duckdb_file_path = os.path.join(os.getcwd(), "poesie_francaise.duckdb")
        else:
            self.duckdb_file_path = duckdb_file_path
        self.engine = create_engine(f"duckdb:///{self.duckdb_file_path}")
        self.logger.info(f"DuckDB file : {self.duckdb_file_path}")

    def fetch_poets(self):
        self.logger.info("**** fetch poets ****")
        start_time_step = time.perf_counter()

        # init
        poet_slugs = []
        poet_names = []
        poet_dobs = []
        poet_dods = []

        # Parse the HTML using BeautifulSoup
        response = requests.get(self.poet_root_url)
        html_content = response.text
        soup = BeautifulSoup(html_content, "html.parser")

        # Find all <ul> elements with class "reglage-menu"
        ul_elements = soup.find_all("ul", class_="reglage-menu")

        # Extract information from each <li> element
        for ul_element in ul_elements:
            # Find all <li> elements within the current <ul>
            poet_elements = ul_element.find_all("li")

            # Extract information from each <li> element
            for poet_element in poet_elements:
                # Find the first <a> tag within the <li> element
                a_tag = poet_element.find("a")

                # Check if the <a> tag exists and has an "href" attribute
                if a_tag and "href" in a_tag.attrs:
                    # Extract the href attribute value
                    href_value = a_tag["href"]

                    # Extract the substring after "https://www.poesie-francaise.fr/poemes-"
                    poet_slug_match = re.search(
                        r"https://www\.poesie-francaise\.fr/poemes-(.*?)/", href_value
                    )

                    if poet_slug_match:
                        poet_slug = poet_slug_match.group(1)

                        # Extract the poet's name and date of birth-death
                        poet_name_and_dates = a_tag.text

                        # Try to split the poet's name and dates
                        match = re.match(r"(.+?) \((\d+-\d+)\)", poet_name_and_dates)
                        if match:
                            poet_name, poet_dates = match.groups()
                            poet_name = poet_name.strip()
                            poet_dob, poet_dod = poet_dates.split("-")
                            poet_slugs.append(poet_slug)
                            poet_names.append(poet_name)
                            poet_dobs.append(poet_dob)
                            poet_dods.append(poet_dod)

        poets = pd.DataFrame(
            list(zip(poet_slugs, poet_names, poet_dobs, poet_dods)),
            columns=["poet_slug", "poet_name", "poet_dob", "poet_dod"],
        )
        poets.to_sql(name="poets", con=self.engine, if_exists="replace", index=False)

        elapsed_time_s_step = time.perf_counter() - start_time_step
        self.logger.info(
            f"{'fetch_poets':30s} - Elapsed time (s) : {elapsed_time_s_step:10.3f}"
        )

    def fetch_poems(self, chunk_size=100):
        self.logger.info("**** fetch poems ****")
        start_time_step = time.perf_counter()

        # init
        poet_slugs = []
        poem_titles = []
        poet_names = []
        poem_books = []
        poem_texts = []
        if_exists = "replace"

        sql = "SELECT poet_slug FROM poets"
        poet_slugs_series = pd.read_sql(sql=sql, con=self.engine).poet_slug

        # loop on poet slugs
        poem_count = 0
        for i, poet_slug in enumerate(poet_slugs_series.values):
            self.logger.info(f"poet slug : {poet_slug}")
            poet_root_url = self.poem_root_url + poet_slug
            response = requests.get(poet_root_url)
            poet_html_content = response.text

            # Parse the HTML using re
            pattern = rf'<a\s+href="(https://www\.poesie-francaise\.fr/{poet_slug}/(?:poeme|fable)-.*?\.php)"\s*>'
            matches = re.findall(pattern, poet_html_content)

            # loop on poems
            for j, poem_root_url in enumerate(matches):
                response = requests.get(poem_root_url)
                poem_html_content = response.text

                poet_slugs.append(poet_slug)

                # poem title
                # ----------

                title_pattern = r"<h2>Titre : (.*?)</h2>"
                title_matches = re.findall(title_pattern, poem_html_content, re.DOTALL)
                if len(title_matches) > 1:
                    self.logger.error("found more that one title for a single enrty")
                poem_title = title_matches[0].strip()
                poem_titles.append(poem_title)

                # poet name
                # ---------

                poet_pattern = r'<h3>Poète : <a href=".*?">(.*?)</a>'
                poet_matches = re.findall(poet_pattern, poem_html_content, re.DOTALL)
                if len(poet_matches) > 1:
                    self.logger.error(
                        "found more than one poet name for a single entry"
                    )
                poet_name = poet_matches[0].strip()
                poet_names.append(poem_title)

                # poem book
                # ---------

                book_pattern = r'Recueil : <a href=".*?">(.*?)</a>'
                book_matches = re.findall(book_pattern, poem_html_content, re.DOTALL)
                if len(book_matches) > 1:
                    self.logger.error("found more than one book for a single entry")
                try:
                    poem_book = book_matches[0].strip()
                except Exception as _:
                    book_pattern = r'<div class="w3-margin-bottom">Recueil : (.*?).</p>'
                    book_matches = re.findall(
                        book_pattern, poem_html_content, re.DOTALL
                    )
                    if len(book_matches) > 1:
                        self.logger.error("found more than one book for a single entry")
                    poem_book = book_matches[0].strip()
                self.logger.info(f"{poet_slug}-{poet_name}/{poem_book}/{poem_title}")
                poem_books.append(poem_book)

                # poem text
                # ---------

                poem_pattern = rf'<p>(.*?)</p>\n<a href="https://www.poesie-francaise.fr/poemes-{poet_slug}/">{poet_name}</a>'
                poem_matches = re.findall(poem_pattern, poem_html_content, re.DOTALL)
                poem_text = poem_matches[0].strip()

                # Parse the HTML using BeautifulSoup
                poem_soup = BeautifulSoup(poem_text, "html.parser")

                # Find all <span> elements with class attributes starting with "decalage"
                spans_to_replace = poem_soup.find_all(
                    "span", class_=re.compile(r"decalage\d+")
                )

                # Replace each <span> element with the corresponding number of spaces
                for span in spans_to_replace:
                    # Extract the number of spaces from the class name using regular expression
                    match = re.search(r"decalage(\d+)", span["class"][0])

                    if match:
                        spaces = int(match.group(1))
                        replacement = poem_soup.new_string(" " * spaces)
                        span.replace_with(replacement)

                poem_text = str(poem_soup)
                poem_text = poem_text.replace("<br />", "\n")
                poem_text = poem_text.replace("<br/>", "\n")
                poem_text = poem_text.replace(" ;", ";")

                # Replace multiple consecutive blank lines with a single one
                poem_text = re.sub(r"\n\s*\n", "\n\n", poem_text)

                poem_texts.append(poem_text)

                if poem_count % chunk_size == 0:
                    poems = pd.DataFrame(
                        list(
                            zip(
                                poet_slugs,
                                poem_titles,
                                poet_names,
                                poem_books,
                                poem_texts,
                            )
                        ),
                        columns=["poet_slug", "poet_name", "poet_dob", "poet_dod"],
                    )
                    poems.to_sql(
                        name="poems", con=self.engine, if_exists=if_exists, index=False
                    )
                    poet_slugs = []
                    poem_titles = []
                    poet_names = []
                    poem_books = []
                    poem_texts = []
                    if_exists = "append"

                poem_count += 1

        if len(poet_slugs) > 0:
            poems = pd.DataFrame(
                list(zip(poet_slugs, poem_titles, poet_names, poem_books, poem_texts)),
                columns=["poet_slug", "poet_name", "poet_dob", "poet_dod"],
            )
            poems.to_sql(
                name="poems", con=self.engine, if_exists=if_exists, index=False
            )

        elapsed_time_s_step = time.perf_counter() - start_time_step
        self.logger.info(
            f"{'fetch_poems':30s} - Elapsed time (s) : {elapsed_time_s_step:10.3f}"
        )


if __name__ == "__main__":
    scraper = Scraper()
    # scraper.fetch_poets()
    scraper.fetch_poems()
