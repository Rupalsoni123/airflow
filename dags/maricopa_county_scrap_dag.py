import json
import logging
import os
import base64
import time
from datetime import timedelta
import requests
from datetime import datetime

from bs4 import BeautifulSoup
from dotenv import load_dotenv

import pendulum
import pandas as pd
from pydantic import BaseModel

from airflow.sdk import Param
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from openai import OpenAI
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


from typing import Dict, List
import re

load_dotenv()

logger = logging.getLogger(__name__)


def extract_parcel_page_json(soup: BeautifulSoup) -> Dict[str, object]:
    """Return all key data blocks as JSON‑ready dicts/lists."""
    # ----------------------------------------------------------------------
    # 1.  PROPERTY INFORMATION  (unchanged)
    # ----------------------------------------------------------------------

    logger.info(" -- extract_parcel_page_json ---")
    prop_json: Dict[str, str] = {}
    prop_panel = soup.select_one(
        "#cphMainContent_cphRightColumn_ParcelNASitusLegal_pnlPropertyInfo"
    )
    if prop_panel:
        for h5 in prop_panel.select("h5"):
            heading = h5.get_text(" ", strip=True)
            div = h5.find_next("div")
            if div:
                prop_json[heading] = " ".join(div.stripped_strings)

    # ----------------------------------------------------------------------
    # 2.  TAX DETAILS  (unchanged)
    # ----------------------------------------------------------------------
    tax_json: Dict[str, str] = {}
    details_panel = soup.select_one("#cphMainContent_cphRightColumn_divDetails .panel")

    if details_panel:
        # We'll build two vertical buckets: left column & right column
        left_labels, right_labels = [], []
        left_value = right_value = ""

        for row in details_panel.select("div.row"):
            # each data row has exactly two (label, span) pairs
            lbls = row.select("label")
            spans = row.select("span")

            if len(lbls) == 2 and len(spans) == 2:
                # left pair
                left_labels.append(lbls[0].get_text(" ", strip=True))
                if not left_value:
                    left_value = spans[0].get_text(" ", strip=True)

                # right pair
                right_labels.append(lbls[1].get_text(" ", strip=True))
                if not right_value:
                    right_value = spans[1].get_text(" ", strip=True)

        # join the labels in each column and map to the first non‑empty value found
        if left_labels:
            tax_json[" ".join(left_labels)] = left_value
        if right_labels:
            tax_json[" ".join(right_labels)] = right_value

    # ----------------------------------------------------------------------
    # 3.  AMOUNTS PAID  (NEW)
    # ----------------------------------------------------------------------
    amounts_json: Dict[str, str] = {}
    amounts_header = soup.find("h4", string=re.compile(r"Amounts\s+Paid", re.I))
    if amounts_header:
        # walk to the sibling '.panel' containing rows
        panel = amounts_header.find_next("div", class_="panel")
        if panel:
            for row in panel.select("div.row"):
                label_tag = row.select_one("label")
                val_tag = row.select_one("span, a")
                if label_tag and val_tag:
                    label = label_tag.get_text(" ", strip=True)
                    value = val_tag.get_text(" ", strip=True)
                    amounts_json[label] = value

    # ----------------------------------------------------------------------
    # 4.  SPECIAL TAX DISTRICTS  (NEW)
    # ----------------------------------------------------------------------
    districts: List[Dict[str, str]] = []
    dist_header = soup.find("h4", string=re.compile(r"Special\s+Tax\s+Districts", re.I))
    if dist_header:
        table = dist_header.find_next("table")
        if table:
            headers = [th.get_text(" ", strip=True) for th in table.select("thead th")]
            for tr in table.select("tbody tr"):
                cells = [td.get_text(" ", strip=True) for td in tr.select("td")]
                if len(cells) == len(headers):
                    districts.append(dict(zip(headers, cells)))

        # ----------------------------------------------------------------------
    # 5.  TAX PERCENTAGES  (NEW)
    # ----------------------------------------------------------------------
    percentages_json: Dict[str, str] = {}
    perc_header = soup.find("h4", string=re.compile(r"Tax\s+Percentages", re.I))
    if perc_header:
        # the grey panel that follows the <h4>
        panel = perc_header.find_next("div", class_="panel")
        if panel:
            for row in panel.select("div.row"):
                label_tag = row.select_one("label")
                value_tag = row.select_one("span")
                if label_tag and value_tag:
                    label = label_tag.get_text(" ", strip=True)
                    value = value_tag.get_text(" ", strip=True)
                    percentages_json[label] = value

    # if there's only one district, convert list → single dict (matches your sample)
    if len(districts) == 1:
        districts = districts[0]

    return {
        "property_info": prop_json,
        "tax_details": tax_json,
        "amounts_paid": amounts_json,
        "special_tax_districts": districts,
        "tax_percentages": percentages_json,
    }


# support function for -> mcassessor_maricopa_gov,
def prepare_json_data(soup, table_class: str):
    """
    Prepare json form the html table
    """
    json_data = {}

    # Find the "Property Information" section
    data_section = soup.find("div", class_=table_class)

    if data_section:
        rows = data_section.select(".smaller-font .row.mb-1")
        for row in rows:
            key_elem = row.select_one(".td-header")
            val_elem = row.select_one(".td-body")
            if key_elem and val_elem:
                key = key_elem.get_text(strip=True)
                val = val_elem.get_text(strip=True)
                json_data[key] = val
        return json_data
    return None


# support function for -> mcassessor_maricopa_gov,
def section_class_from_heading(
    soup: BeautifulSoup,
    heading: str,
    skip_if_class_contains=("ribbon",),
):
    """
    Return the class string of the outer 'section' wrapper that contains `heading`.

    Parameters
    ----------
    soup : BeautifulSoup
        A parsed BeautifulSoup document (NOT raw HTML).
    heading : str
        Exact text (case‑insensitive) inside an <h1>…<h6>.
    skip_if_class_contains : Iterable[str>, optional
        Sub‑strings; any ancestor whose class list contains one of these
        will be ignored while climbing.  Defaults to ("ribbon",).

    Returns
    -------
    str | None
        e.g. "extra-section bg-white rounded shadow py-3 px-3 pb-4 mb-4 col-12",
        or None if the heading isn't found.
    """
    # 1️⃣ find the heading tag
    target = heading.strip().casefold()
    hdr = soup.find(
        lambda tag: tag.name in ("h1", "h2", "h3", "h4", "h5", "h6")
        and tag.get_text(strip=True).casefold() == target
    )
    if hdr is None:
        return None

    # 2️⃣ walk up the ancestors until we hit a qualifying <div>
    node = hdr.parent
    while node:
        node = node.parent
        if (
            node
            and node.name == "div"
            and (cls := node.get("class"))
            and not any(skip in cls for skip in skip_if_class_contains)
        ):
            return " ".join(cls)
    return None


class OrderDetails(BaseModel):
    """
    This is the model for the order details that we will be scraping.
    It is used to parse the response from the OpenAI API.
    It is also used to save the order details to a database.
    """

    mcr_id: str
    parcel_id: str
    borrower_name: str
    loan_amount: str
    property_address: str
    county: str
    legal_description: str


class ParcelDetails(BaseModel):
    """
    This is the model for the parcel details that we will be scraping.
    It is used to parse the response from the OpenAI API.
    It is also used to save the parcel details to a database.
    """

    parcel_id: str


@dag(
    dag_id="maricopa_county_scrap_dag",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
    params={"order_id": Param(358647)},
)
def maricopa_county_scrap_dag():
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)
    """

    @task()
    def load_order_data(**kwargs):
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline. In this case, getting data is simulated by reading from a
        hardcoded JSON string.
        """
        mysql_hook = MySqlHook(mysql_conn_id="RTS_DEMO_DB")
        order_df = mysql_hook.get_pandas_df(
            sql=f"""
            SELECT
             orders_orders.id as order_id,
             orders_orders.universal_rts_number as universal_rts_number,
             orders_orders.first_borrower,
             orders_orders.second_borrower,
             addresses_addresses.street as street,
             addresses_addresses.zip_code as zip_code,
             addresses_cities.name as city,
             addresses_counties.name as county,
             addresses_states.name as state,
             addresses_countries.name as country
            FROM orders_orders
            LEFT JOIN addresses_addresses
            ON orders_orders.address_id = addresses_addresses.id
            LEFT JOIN addresses_countries
            ON addresses_addresses.country_id = addresses_countries.id
            LEFT JOIN addresses_states
            ON addresses_addresses.state_id = addresses_states.id
            LEFT JOIN addresses_counties
            ON addresses_addresses.county_id = addresses_counties.id
            LEFT JOIN addresses_cities
            ON addresses_addresses.city_id = addresses_cities.id
            WHERE orders_orders.id = {kwargs["params"]["order_id"]}
            """
        )

        order_notes_df = mysql_hook.get_pandas_df(
            sql=f"""
            SELECT note FROM orders_ordernotes
            WHERE order_id = {kwargs["params"]["order_id"]}
            ORDER BY created_on DESC
            LIMIT 2
            """
        )

        return {
            "order_id": order_df.iloc[0]["order_id"],
            "universal_rts_number": order_df.iloc[0]["universal_rts_number"],
            "first_borrower": order_df.iloc[0]["first_borrower"],
            "second_borrower": order_df.iloc[0]["second_borrower"],
            "street": order_df.iloc[0]["street"],
            "zip_code": order_df.iloc[0]["zip_code"],
            "city": order_df.iloc[0]["city"],
            "county": order_df.iloc[0]["county"],
            "state": order_df.iloc[0]["state"],
            "country": order_df.iloc[0]["country"],
            "order_notes": order_notes_df.iloc[1]["note"],
        }

    @task()
    def extract_order_details(order_data_dict: dict):
        """
        #### Transform task
        A simple Transform task which takes in the collection of order data and
        computes the total order value.
        """
        client = OpenAI(
            api_key="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"
        )

        response = client.responses.parse(
            model="gpt-4.1-mini",
            instructions="You are profession US tax assessor that can understand crtical information like MCR iD , parcel id , borrower name etc",
            input=f"""
            Please provide the following information:
            - MCR iD
            - Parcel ID <A 8 digit number with names like property index>
            - Borrower Name
            - Loan Amount
            - Property Address
            - County
            - Legal Description

            {order_data_dict}
            """,
            text_format=OrderDetails,
        )

        logger.info(response.output_parsed)

        return {
            "order_data": order_data_dict,
            "order_details": response.output_parsed.model_dump(mode="dict"),
        }

        # return {
        #     "order_details": {
        #         "mcr_id": "74889987",
        #         "parcel_id": "10448282",
        #         "borrower_name": "Autumn Crawford",
        #         "loan_amount": "$129,000.00",
        #         "property_address": "6325 W Crown King Rd, Phoenix, AZ 85043-7791",
        #         "county": "Maricopa",
        #         "legal_description": "ACRES: 6 277 2E 19 1N",
        #     },
        #     "order_data": order_data_dict,
        # }

    @task(
        retries=1,
        retry_delay=timedelta(seconds=10),
        task_id="scrape_with_borrower_name",
    )
    def scrape_with_borrower_name(order_details_dict: dict):
        """
        #### Save task
        A simple Save task which takes in the collection of order details and
        saves it to a database.
        """

        borrower_name = order_details_dict["order_data"]["first_borrower"]

        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")

        driver = webdriver.Remote(
            command_executor="remote_chromedriver:4444/wd/hub",
            options=options,
        )

        logger.info(f"Driver Strated for borrower name: {borrower_name}")

        try:
            driver.get(f"https://mcassessor.maricopa.gov/mcs/?q={borrower_name}")

            # sleep for 10 seconds
            time.sleep(15)

            logger.info(f"Page loaded for borrower name: {borrower_name}")

            # 4. Get the HTML content
            html_content = driver.page_source
            soup = BeautifulSoup(html_content, "html.parser")

            # 5. Remove the script tag
            for script in soup(["script", "style"]):
                script.decompose()

            # 6. Get the text
            rpdetails = soup.find("div", id="rpdetails")

            logger.info(rpdetails.text)

            return {
                "scrape_with_borrower_name_dict": rpdetails.text,
                "order_details_dict": order_details_dict,
            }

        finally:
            # 5. Close the browser
            driver.quit()
            print("Browser session closed.")

    @task(
        retries=1,
        retry_delay=timedelta(seconds=10),
        task_id="scrape_with_address",
    )
    def scrape_with_address(order_details_dict: dict):
        """
        #### Save task
        A simple Save task which takes in the collection of order details and
        saves it to a database.
        """

        street = order_details_dict["order_data"]["street"]
        zip_code = order_details_dict["order_data"]["zip_code"]
        city = order_details_dict["order_data"]["city"]
        county = order_details_dict["order_data"]["county"]
        state = order_details_dict["order_data"]["state"]
        country = order_details_dict["order_data"]["country"]

        full_address = f"{street}, {city}, {county}, {state}, {country}"

        logger.info(full_address)

        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")

        # Use webdriver-manager to automatically download and manage ChromeDriver

        driver = webdriver.Remote(
            command_executor="remote_chromedriver:4444/wd/hub",
            options=options,
        )

        logger.info(f"Driver Strated for address: {full_address}")

        try:
            driver.get(f"https://mcassessor.maricopa.gov/mcs/?q={full_address}")

            # sleep for 10 seconds
            time.sleep(15)

            logger.info(f"Page loaded for address: {full_address}")

            # 4. Get the HTML content
            html_content = driver.page_source
            soup = BeautifulSoup(html_content, "html.parser")

            # 5. Remove the script tag
            for script in soup(["script", "style"]):
                script.decompose()

            # 6. Get the div wiht id rpdetails
            rpdetails = soup.find("div", id="rpdetails")

            logger.info(rpdetails.text)

            return {
                "scrape_with_address_dict": rpdetails.text,
                "order_details_dict": order_details_dict,
            }

        finally:
            # 5. Close the browser
            driver.quit()
            print("Browser session closed.")

    @task()
    def transform_scrape_details(
        scrape_with_borrower_name_dict: dict,
        scrape_with_address_dict: dict,
    ):
        """
        #### Save task
        A simple Save task which takes in the collection of order details and
        saves it to a database.
        """

        cleaned_scrape_with_borrower_name_dict = scrape_with_borrower_name_dict[
            "scrape_with_borrower_name_dict"
        ].replace("\n", " ")
        cleaned_scrape_with_address_dict = scrape_with_address_dict[
            "scrape_with_address_dict"
        ].replace("\n", " ")

        logger.info(cleaned_scrape_with_borrower_name_dict)
        logger.info(cleaned_scrape_with_address_dict)

        order_details_dict = scrape_with_address_dict["order_details_dict"]

        logger.info(order_details_dict)

        client = OpenAI(
            api_key="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"
        )

        response = client.responses.parse(
            model="gpt-4.1-mini",
            instructions="You are profession US tax assessor that can understand crtical information like MCR iD , parcel id , borrower name etc",
            input=f"""
                Input : Read the details provided in order details
                Context: Contains Data regarding the order and its purchase
                {order_details_dict}

                Input : Read the details scraped
                Context : Contains List of APN data scraped from official site which need to be compared with order details
                {scrape_with_borrower_name_dict}
                {scrape_with_address_dict}


                You task is to compare the order details given with scrape data and Return the APN number from scrape data
                as parcel id which matches the details given in order details

                Rule:
                1. Return the APN from scrape data only which matches the order details


            """,
            text_format=ParcelDetails,
        )

        logger.info(response.output_parsed)

        return {
            "parcel_id": response.output_parsed.parcel_id,
            "order_details_dict": order_details_dict,
        }

    @task(
        retries=1,
        retry_delay=timedelta(seconds=10),
        task_id="scrape_assessor_details",
    )
    def scrape_assessor_details(assessor_parcel_details_dict: dict):
        """
        #### Save task
        A simple Save task which takes in the collection of order details and
        saves it to a database.
        """

        logger.info(assessor_parcel_details_dict)

        parcel_id: str = (
            assessor_parcel_details_dict["parcel_id"].replace("-", "").strip()
        )
        order_details_dict: dict = assessor_parcel_details_dict["order_details_dict"]

        logger.info(parcel_id)
        logger.info(order_details_dict)

        universal_rts_number: str = order_details_dict["order_data"][
            "universal_rts_number"
        ]

        logger.info(universal_rts_number)

        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")

        # Use webdriver-manager to automatically download and manage ChromeDriver

        driver = webdriver.Remote(
            command_executor="remote_chromedriver:4444/wd/hub",
            options=options,
        )

        logger.info(f"Driver Strated for assessor details: {parcel_id}")

        try:
            driver.get(f"https://mcassessor.maricopa.gov/mcs/?q={parcel_id}")

            logger.info(f"Page loaded for assessor details: {parcel_id}")

            # wait for property-glance to be present
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.ID, "property-glance"))
            )

            logger.info(f"waited for property-glance to be present")

            # Some SPA sites keep fetching data after readyState == "complete".
            # If you suspect that, you can chain a second lambda that waits for no active network:
            WebDriverWait(driver, 45).until(
                lambda d: d.execute_script(
                    "return (window.performance.getEntriesByType('resource')"
                    ".filter(r => !r.responseEnd || r.responseEnd > performance.now() - 500).length === 0)"
                )
            )

            # 4. Get the HTML content
            html_content = driver.page_source
            logger.info(
                " --- html_content --- ",
            )
            soup = BeautifulSoup(html_content, "html.parser")

            # 5. Remove the script tag
            for script in soup(["script", "style"]):
                script.decompose()
            logger.info(
                " --- soup --- ",
            )

            data_section_names = [
                "Property Information",
                "Owner Information",
                "Additional Property Information",
            ]

            assessor_details_dict = {}
            for data_section in data_section_names:
                css_classes = section_class_from_heading(
                    soup, data_section
                )  # function which will give us the class name for the WebSite title
                if css_classes:
                    json_data = prepare_json_data(soup, css_classes)
                    assessor_details_dict[data_section] = json_data
                    logger.info(f"{data_section}: ")
                    logger.info(json_data)
                else:
                    logger.info(f"Couldn't find {data_section} section in the page.")

            # take screenshot of the full page
            # Get original size
            original_size = driver.get_window_size()

            logger.info(f"original_size: {original_size}")
            # Get scroll height
            total_height = driver.execute_script(
                "return document.body.parentNode.scrollHeight"
            )

            logger.info(f"total_height: {total_height}")
            # Set window size to full page size
            driver.set_window_size(original_size["width"], total_height)

            logger.info(f"window_size: {driver.get_window_size()}")

            time.sleep(2)  # Allow time for resizing and rendering

            base64_string = driver.get_screenshot_as_base64()

            logger.info(f"base64_string length: {len(base64_string)}")

            # convert bytes to base64 string
            image_data = base64.b64decode(base64_string)

            logger.info(f"image_data length: {len(image_data)}")

            s3_hook = S3Hook(aws_conn_id="RTS_QA_S3")

            screenshot_key = f"qa/tax_assessor_scrape/{universal_rts_number}_parcel.png"

            s3_hook.load_bytes(
                bytes_data=image_data,
                key=screenshot_key,
                bucket_name="rts3.0",
                replace=True,
            )

            # get the map id from page using soup
            # the map id is in css class mapid-file
            map_id = soup.find("span", class_="mapid-file").text.strip()
            logger.info(f"map_id : {map_id}")

            # store the pdf in s3 also
            pdf_key = f"qa/tax_assessor_scrape/{universal_rts_number}_map.pdf"
            pdf_url = f"https://mcassessor.maricopa.gov/getmapid/{map_id}/"
            pdf_data = requests.get(pdf_url).content

            logger.info(f"pdf_data : {type(pdf_data)}")

            s3_hook.load_bytes(
                bytes_data=pdf_data,
                key=pdf_key,
                bucket_name="rts3.0",
                replace=True,
            )

            return {
                "order_details_dict": order_details_dict,
                "assessor_details_dict": assessor_details_dict,
                "file_data": [
                    {
                        "name": screenshot_key,
                        "type": "image",
                        "location": "main",
                    },
                    {
                        "name": pdf_key,
                        "type": "pdf",
                        "location": "parcel_map",
                    },
                ],
            }

        except Exception as e:
            logger.error(f"Error scraping assessor details: {e}")
            raise e

        finally:
            # 5. Close the browser
            driver.quit()
            print("Browser session closed.")

    @task(
        retries=1,
        retry_delay=timedelta(seconds=10),
        task_id="scrape_tax_details",
    )
    def scrape_tax_details(assessor_parcel_details_dict: dict):
        """
        #### Save task
        A simple Save task which takes in the collection of order details and
        saves it to a database.
        """

        parcel_id: str = (
            assessor_parcel_details_dict["parcel_id"].replace("-", "").strip()
        )
        order_details_dict: dict = assessor_parcel_details_dict["order_details_dict"]

        universal_rts_number: str = order_details_dict["order_data"][
            "universal_rts_number"
        ]

        logger.info(parcel_id)

        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")

        # Use webdriver-manager to automatically download and manage ChromeDriver

        driver = webdriver.Remote(
            command_executor="remote_chromedriver:4444/wd/hub",
            options=options,
        )

        logger.info(f"Driver Strated for tax details: {parcel_id}")

        try:
            driver.get(
                f"https://treasurer.maricopa.gov/parcel/default.aspx?Parcel={parcel_id}"
            )

            logger.info(f"Page loaded for tax details: {parcel_id}")

            # wait for the page to load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(
                    (
                        By.ID,
                        "cphMainContent_cphRightColumn_ParcelNASitusLegal_pnlPropertyInfo",
                    )
                )
            )

            # click on this xpath button //*[@id="siteInnerContentContainer"]/div/div[2]/div[2]/div[2]/div[3]/div[1]/div[2]/p/a
            button = driver.find_element(
                By.XPATH,
                "//*[@id='siteInnerContentContainer']/div/div[2]/div[2]/div[2]/div[3]/div[1]/div[2]/p/a",
            )
            button.click()

            # wait for the page to load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(
                    (By.ID, "cphMainContent_cphRightColumn_divDetails")
                )
            )

            # 4. Get the HTML content
            html_content = driver.page_source
            soup = BeautifulSoup(html_content, "html.parser")

            # 5. Remove the script tag
            for script in soup(["script", "style"]):
                script.decompose()

            logger.info(
                " prepare_json_data start",
            )
            page_json = extract_parcel_page_json(soup)
            tax_details_dict = page_json
            logger.info(json.dumps(page_json))
            logger.info(
                " prepare_json_data end",
            )

            # take screenshot of the full page
            # Get original size
            original_size = driver.get_window_size()
            # Get scroll height
            total_height = driver.execute_script(
                "return document.body.parentNode.scrollHeight"
            )
            # Set window size to full page size
            driver.set_window_size(original_size["width"], total_height)
            time.sleep(2)  # Allow time for resizing and rendering

            base64_string = driver.get_screenshot_as_base64()

            # convert bytes to base64 string
            image_data = base64.b64decode(base64_string)

            s3_hook = S3Hook(aws_conn_id="RTS_QA_S3")

            screenshot_key = f"qa/tax_assessor_scrape/{universal_rts_number}_tax.png"

            s3_hook.load_bytes(
                bytes_data=image_data,
                key=screenshot_key,
                bucket_name="rts3.0",
                replace=True,
            )

            return {
                "order_details_dict": order_details_dict,
                "tax_details_dict": tax_details_dict,
                "file_data": [
                    {
                        "name": screenshot_key,
                        "type": "image",
                        "location": "main",
                    },
                ],
            }

        except Exception as e:
            if driver:
                driver.quit()
                print("Browser session closed.")

            logger.error(f"Error scraping tax details: {e}")
            raise e
        finally:
            if driver:
                driver.quit()
                print("Browser session closed.")

    @task(
        retries=1,
        retry_delay=timedelta(seconds=10),
        task_id="save_tax_assessor_details",
    )
    def load_tax_assessor_details(assessor_details_dict: dict, tax_details_dict: dict):
        """
        #### Save task
        A simple Save task which takes in the collection of order details and
        saves it to a database.
        """

        """
        class OrderTaxAssessorData(models.Model):

            order = models.ForeignKey(
                Orders,
                on_delete=models.CASCADE,
                related_name="tax_assessor_data",
            )
            site_type = models.CharField(max_length=10)

            data_type = models.CharField(max_length=10)
            json_data = models.JSONField(null=True, blank=True)
            file_path = models.CharField(max_length=255, null=True, blank=True)

            content_location = models.CharField(max_length=255, null=True, blank=True)
            raw_data = models.TextField(null=True, blank=True)
            created_on = models.DateTimeField(auto_now_add=True)
        """
        logger.info(f"assessor_details_dict: {assessor_details_dict}")
        logger.info(f"tax_details_dict: {tax_details_dict}")

        order_id = assessor_details_dict["order_details_dict"]["order_data"]["order_id"]
        mysql_hook = MySqlHook(mysql_conn_id="RTS_DEMO_DB")

        # Prepare and insert assessor data
        assessor_rows_to_insert = []
        assessor_json_row = {
            "order_id": order_id,
            "site_type": "assessor",
            "data_type": "json",
            "json_data": json.dumps(assessor_details_dict["assessor_details_dict"]),
            "file_path": None,
            "content_location": "main",
            "raw_data": None,
            "created_on": datetime.now(),
        }
        assessor_rows_to_insert.append(tuple(assessor_json_row.values()))

        for file_info in assessor_details_dict.get("file_data", []):
            assessor_file_row = {
                "order_id": order_id,
                "site_type": "assessor",
                "data_type": file_info.get("type"),
                "json_data": None,
                "file_path": file_info.get("name"),
                "content_location": file_info.get("location"),
                "raw_data": None,
                "created_on": datetime.now(),
            }
            assessor_rows_to_insert.append(tuple(assessor_file_row.values()))

        if assessor_rows_to_insert:
            mysql_hook.insert_rows(
                table="orders_ordertaxassessordata",
                rows=assessor_rows_to_insert,
                target_fields=list(assessor_json_row.keys()),
            )
            logger.info("Inserted assessor data.")

        # Prepare and insert tax data
        tax_rows_to_insert = []
        tax_json_row = {
            "order_id": order_id,
            "site_type": "tax",
            "data_type": "json",
            "json_data": json.dumps(tax_details_dict["tax_details_dict"]),
            "file_path": None,
            "content_location": "main",
            "raw_data": None,
            "created_on": datetime.now(),
        }
        tax_rows_to_insert.append(tuple(tax_json_row.values()))

        for file_info in tax_details_dict.get("file_data", []):
            tax_file_row = {
                "order_id": order_id,
                "site_type": "tax",
                "data_type": file_info.get("type"),
                "json_data": None,
                "file_path": file_info.get("name"),
                "content_location": file_info.get("location"),
                "raw_data": None,
                "created_on": datetime.now(),
            }
            tax_rows_to_insert.append(tuple(tax_file_row.values()))

        if tax_rows_to_insert:
            mysql_hook.insert_rows(
                table="orders_ordertaxassessordata",
                rows=tax_rows_to_insert,
                target_fields=list(tax_json_row.keys()),
            )
            logger.info("Inserted tax data.")

    order_data = load_order_data()
    order_details = extract_order_details(order_data)
    scrape_with_borrower_name_dict = scrape_with_borrower_name(order_details)
    scrape_with_address_dict = scrape_with_address(order_details)
    assessor_parcel_details = transform_scrape_details(
        scrape_with_borrower_name_dict, scrape_with_address_dict
    )
    assessor_details = scrape_assessor_details(assessor_parcel_details)
    tax_details = scrape_tax_details(assessor_parcel_details)
    load_tax_assessor_details(assessor_details, tax_details)


maricopa_county_scrap_dag()
