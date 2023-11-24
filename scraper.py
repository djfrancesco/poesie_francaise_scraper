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
from slugify import slugify
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

    def _read_poem_count(self, poet_name, html_content):
        # Construct the regex pattern with a capturing group for the integer
        pattern = re.compile(
            rf"<h2>(?:[^\n\d]*(\d+)\s*-\s*)?Les (\d+) (?:poèmes|fables|(?:poèmes\s+et\s+fables)) d[e']?.*?{re.escape(poet_name)}\s*:</h2>",
            re.IGNORECASE,
        )

        # Search for the pattern in the HTML content
        match = pattern.search(html_content)

        if match:
            # Extract the integer from the captured group
            poem_count = int(match.group(2))
        else:
            poem_count = 0

        return poem_count

    def _find_next_page_link(self, soup):
        next_pages_div = soup.find("div", class_="nextpages")
        if next_pages_div:
            next_page_link = next_pages_div.find(
                "a", string=re.compile("suivante", re.IGNORECASE), href=True
            )
            if next_page_link:
                return next_page_link["href"]
        return None

    def _fetch_poems(self, html_content, poet_slug, if_exists="append"):
        poet_slugs = []
        poem_titles = []
        poem_slugs = []
        poet_names = []
        poem_books = []
        poem_texts = []

        pattern = rf'<a\s+href="(https://www\.poesie-francaise\.fr/{poet_slug}/(?:poeme|fable)-.*?\.php)"\s*>'
        matches = re.findall(pattern, html_content)
        match_count = len(matches)

        # loop on poems from a single poet
        for _, poem_root_url in enumerate(matches):
            response = requests.get(poem_root_url)
            poem_html_content = response.text

            poet_slugs.append(poet_slug)

            # poem title
            title_pattern = r"<h2>Titre : (.*?)</h2>"
            title_matches = re.findall(title_pattern, poem_html_content, re.DOTALL)
            poem_title = title_matches[0].strip()
            poem_titles.append(poem_title)

            # poem slugs
            poem_slug = slugify(poem_title)
            poem_slugs.append(poem_slug)

            # poet name
            poet_pattern = r'<h3>Poète : <a href=".*?">(.*?)</a>'
            poet_matches = re.findall(poet_pattern, poem_html_content, re.DOTALL)
            poet_name = poet_matches[0].strip()
            poet_names.append(poet_name)

            # poem book
            book_pattern = r'Recueil : <a href=".*?">(.*?)</a>'
            book_matches = re.findall(book_pattern, poem_html_content, re.DOTALL)
            try:
                poem_book = book_matches[0].strip()
            except Exception as _:
                book_pattern = r'<div class="w3-margin-bottom">Recueil : (.*?).</p>'
                book_matches = re.findall(book_pattern, poem_html_content, re.DOTALL)
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

            # Remove remaining HTML tags
            poem_text = re.sub(r"<.*?>", "", poem_text)

            poem_texts.append(poem_text)

        poems = pd.DataFrame(
            list(
                zip(
                    poet_slugs,
                    poem_titles,
                    poem_slugs,
                    poet_names,
                    poem_books,
                    poem_texts,
                )
            ),
            columns=[
                "poet_slug",
                "poem_title",
                "poem_slugs",
                "poet_name",
                "poem_book",
                "poem_text",
            ],
        )
        poems.to_sql(name="poems", con=self.engine, if_exists=if_exists, index=False)

        return match_count

    def fetch_poems(self):
        self.logger.info("**** fetch poems ****")
        start_time_step = time.perf_counter()

        # init
        if_exists = "replace"

        # get full list of poets
        sql = "SELECT poet_slug, poet_name FROM poets"
        poets = pd.read_sql(sql=sql, con=self.engine)

        # loop on poets
        for row in poets.itertuples():
            poet_slug = row.poet_slug
            poet_name = row.poet_name
            self.logger.info(f"poet name : '{poet_name}', slug : '{poet_slug}'")

            # fetch html content of first author page
            poet_root_url = self.poem_root_url + poet_slug
            response = requests.get(poet_root_url)
            html_content = response.text

            # read number of poems from poet from first author page
            poem_count = self._read_poem_count(poet_name, html_content)
            self.logger.info(f"{poem_count} poems")

            # loop on poet pages
            url = poet_root_url
            match_count = 0
            scanned_urls = []
            while url and (url not in scanned_urls):
                scanned_urls.append(url)

                match_count += self._fetch_poems(
                    html_content, poet_slug, if_exists=if_exists
                )
                if_exists = "append"

                # Find the link to the next page, if exists
                soup = BeautifulSoup(html_content, "html.parser")
                next_page_link = self._find_next_page_link(soup)
                if next_page_link:
                    url = next_page_link
                    response = requests.get(url)
                    html_content = response.text
                else:
                    url = None

            if match_count != poem_count:
                self.logger.error(
                    f"poem_count : {poem_count}, match_count : {match_count}"
                )
            # assert match_count == poem_count

        elapsed_time_s_step = time.perf_counter() - start_time_step
        self.logger.info(
            f"{'fetch_poems':30s} - Elapsed time (s) : {elapsed_time_s_step:10.3f}"
        )

    def fetch_all(self, mode="drop"):
        if mode == "drop":
            if os.path.exists(self.duckdb_file_path):
                os.remove(self.duckdb_file_path)

        self.fetch_poets()
        self.fetch_poems()


if __name__ == "__main__":
    scraper = Scraper()
    scraper.fetch_all()
