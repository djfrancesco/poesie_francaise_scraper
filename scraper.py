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
    start_url = "https://www.poesie-francaise.fr/poemes-"
    authors_url = "https://www.poesie-francaise.fr/poemes-auteurs/"

    def __init__(self, duckdb_file_path=None, log_level="INFO"):
        self.logger = create_logger(log_level=log_level)
        self.logger.info("**** poesie francaise scraper init ****")
        warnings.filterwarnings("ignore", category=DuckDBEngineWarning)
        if duckdb_file_path is None:
            self.duckdb_file_path = os.path.join(os.getcwd(), "poesie_francaise.duckdb")
        else:
            self.duckdb_file_path = duckdb_file_path
        self.engine = create_engine("duckdb:///{self.duckdb_file_path}")
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
        response = requests.get(self.authors_url)
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


if __name__ == "__main__":
    scraper = Scraper()
    scraper.fetch_poets()
    scraper.fetch_poems()
