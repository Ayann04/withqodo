import os
import re
import time
import logging
from io import BytesIO
from datetime import datetime
import shutil
from django.conf import settings
from django.core.cache import cache
from django.core.files.base import ContentFile
from django.db import transaction
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render

from PIL import Image
from openpyxl import Workbook

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait

try:
    # Used for local/dev fallback when driver path is not provided
    from webdriver_manager.chrome import ChromeDriverManager
except Exception:
    ChromeDriverManager = None

from .models import ScrapingRun, ScrapingStatus, ScrapedRecord

logger = logging.getLogger(__name__)

# Configurable constants
DEFAULT_WAIT = int(getattr(settings, "SELENIUM_DEFAULT_WAIT", 30))
CAPTCHA_WAIT_SECONDS = int(getattr(settings, "CAPTCHA_WAIT_SECONDS", 180))
# Cache key template for per-run CAPTCHA values
CAPTCHA_CACHE_KEY = "captcha:run:{run_id}"

timestamp_2 = datetime.now().strftime("%Y%m%d_%H%M%S")


def get_status(request):
    """
    Renders current scraping status for the latest run.
    Accepts POST with 'captcha_value' to feed the scraping flow.
    """
    latest_run = ScrapingRun.objects.order_by("-started_at").first()
    if latest_run:
        statuses = latest_run.statuses.order_by("created_at")
    else:
        statuses = []

    latest_status = ScrapingStatus.objects.order_by("-created_at").first()

    captcha_value = None
    if request.method == "POST":
        captcha_value = (request.POST.get("captcha_value") or "").strip()
        if captcha_value and latest_run:
            cache.set(CAPTCHA_CACHE_KEY.format(run_id=latest_run.id), captcha_value, timeout=300)
            logger.info("Received captcha value for run=%s", latest_run.id)
        elif not captcha_value:
            logger.warning("Empty captcha submitted")

    timestamp = int(time.time())
    return render(
        request,
        "scraper_app/status.html",
        {
            "statuses": statuses,
            "status": latest_status,
            "captcha_value": captcha_value,
            "timestamp": timestamp,
        },
    )


def parse_address(addr: str):
    parsed = {}
    patterns = {
        "Ward/Colony": r"Ward Colony\s*-\s*([^,\.]+)",
        "District": r"Distirct:?\s*([^,\.]+)",
        "Village": r"Village:?\s*([^,\.]+)",
        "Sub-Area/Road": r"Sub-Area\s*:?\s*([^,\.]+)",
        "Tehsil/Locality": r"Tehsil:?\s*([^,\.]+)",
        "PIN Code": r"pin-?(\d{6})",
        "Landmark": r"(\d+\s*m\s+from\s+[^p]+)",
    }

    for key, pattern in patterns.items():
        match = re.search(pattern, addr, re.IGNORECASE)
        if match:
            parsed[key] = match.group(1) if match.lastindex and match.lastindex >= 1 else ""
        else:
            parsed[key] = ""

    parsed["State"] = "Madhya Pradesh" if "Madhya Pradesh" in addr else ""
    parsed["Country"] = "India" if "India" in addr else ""
    return parsed


def _create_status(run: ScrapingRun, message: str, pil_image: Image.Image | None = None) -> ScrapingStatus:
    """
    Create a ScrapingStatus row with optional image stored in captcha_image.
    """
    status = ScrapingStatus.objects.create(run=run, message=message)
    if pil_image is not None:
        buffer = BytesIO()
        pil_image.save(buffer, format="PNG")
        buffer.seek(0)
        status.captcha_image.save(f"captcha_{int(time.time())}.png", ContentFile(buffer.read()), save=True)
    return status


def _driver_from_config():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    chrome_options.binary_location = os.environ.get("CHROME_BIN")

    service = Service(os.environ.get("CHROMEDRIVER_PATH"))
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver


def _screenshot_element(driver: webdriver.Chrome, element) -> Image.Image:
    """
    Take a full-page screenshot and crop the given element accurately using devicePixelRatio.
    """
    # Ensure visibility
    driver.execute_script("arguments[0].scrollIntoView(true);", element)
    time.sleep(0.5)

    dpr = driver.execute_script("return window.devicePixelRatio") or 1
    png = driver.get_screenshot_as_png()
    image = Image.open(BytesIO(png))
    img_width, img_height = image.size

    location = element.location_once_scrolled_into_view
    size = element.size

    left = max(0, int(location["x"] * dpr))
    top = max(0, int(location["y"] * dpr))
    right = min(img_width, int((location["x"] + size["width"]) * dpr))
    bottom = min(img_height, int((location["y"] + size["height"]) * dpr))

    cropped = image.crop((left, top, right, bottom))
    return cropped


def _wait_for_captcha_value(run_id: int, timeout: int = CAPTCHA_WAIT_SECONDS, poll_interval: float = 1.0) -> str | None:
    """
    Poll cache for a per-run captcha value set via get_status POST.
    """
    key = CAPTCHA_CACHE_KEY.format(run_id=run_id)
    waited = 0
    while waited < timeout:
        value = cache.get(key)
        if value:
            # Clear it so subsequent steps don't reuse stale values
            cache.delete(key)
            return value
        time.sleep(poll_interval)
        waited += poll_interval
    return None


def save_to_db(all_sections):
    """
    Persist scraped sections into ScrapedRecord with robust handling.
    """
    try:
        with transaction.atomic():
            ScrapedRecord.objects.create(
                registration_details=dict(zip(all_sections[0][0], all_sections[0][1])),
                seller_details=dict(zip(all_sections[1][0], all_sections[1][1])),
                buyer_details=dict(zip(all_sections[2][0], all_sections[2][1])),
                property_details=dict(zip(all_sections[3][0], all_sections[3][1])),
                khasra_details=dict(zip(all_sections[4][0], all_sections[4][1])),
            )
    except Exception as e:
        logger.exception("Failed to save record to DB: %s", e)
        raise


def trigger_scrape(request):
    """
    Launch the scraping process. For production, consider moving this to a background worker (Celery/RQ).
    """
    new_run = ScrapingRun.objects.create()
    if request.method != "POST":
        return render(request, "scraper_app/scrape_form.html")

    username = (request.POST.get("username") or "").strip()
    password = (request.POST.get("password") or "").strip()
    district = (request.POST.get("district") or "").strip()
    deed_type = (request.POST.get("deed_type") or "").strip()
    date_too = request.POST.get("date_to")
    date_from = request.POST.get("date_from")

    try:
        date_from_fmt = datetime.strptime(date_from, "%Y-%m-%d").strftime("%d-%m-%Y")
        date_to_fmt = datetime.strptime(date_too, "%Y-%m-%d").strftime("%d-%m-%Y")
    except Exception:
        _create_status(new_run, "Invalid date format. Expected YYYY-MM-DD.")
        return JsonResponse({"message": "Invalid date format. Expected YYYY-MM-DD."}, status=400)

    
    try:
        driver = _driver_from_config()
        driver.get("https://example.com")

        # Wait for the language selector and switch to English if available
        try:
            english_links = WebDriverWait(driver, DEFAULT_WAIT).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.ng-star-inserted>a"))
            )
            if len(english_links) >= 3:
                english_links[2].click()
        except Exception:
            logger.info("English switch not found or could not be clicked; continuing.")

        # Login loop with CAPTCHA #1
        max_attempts = 10
        login_success = False
        _create_status(new_run, "Filling Username And Password To login")
        for attempt in range(max_attempts):
            try:
                driver.refresh()
                WebDriverWait(driver, DEFAULT_WAIT).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "input#username"))
                )

                username_input = driver.find_element(By.CSS_SELECTOR, "input#username")
                username_input.clear()
                username_input.send_keys(username)

                password_input = driver.find_element(By.CSS_SELECTOR, "input#password")
                password_input.clear()
                password_input.send_keys(password)

                # CAPTCHA image
                elem = WebDriverWait(driver, DEFAULT_WAIT).until(
                    EC.visibility_of_element_located((By.CSS_SELECTOR, "div.input-group>img"))
                )

                # Screenshot and ask user to solve
                captcha_img_1 = _screenshot_element(driver, elem)
                _create_status(new_run, "Please solve CAPTCHA #1 in the UI", pil_image=captcha_img_1)

                captcha_value = _wait_for_captcha_value(new_run.id, timeout=CAPTCHA_WAIT_SECONDS)
                if not captcha_value:
                    _create_status(new_run, "CAPTCHA #1 timed out waiting for input. Retrying...")
                    continue

                captcha_inputs = driver.find_elements(By.CSS_SELECTOR, "div.input-group>input")
                if len(captcha_inputs) < 3:
                    raise RuntimeError("CAPTCHA input box not found for login form.")

                captcha_inputs[2].click()
                captcha_inputs[2].clear()
                captcha_inputs[2].send_keys(captcha_value)

                # Click login and wait for navigation
                login_button = driver.find_elements(By.CSS_SELECTOR, "button.mat-focus-indicator")
                before_url = driver.current_url
                if len(login_button) >= 2:
                    driver.execute_script("arguments[0].click();", login_button[1])
                else:
                    raise RuntimeError("Login button not found.")

                WebDriverWait(driver, DEFAULT_WAIT).until(EC.url_changes(before_url))
                time.sleep(1.5)  # brief render settle
                after_url = driver.current_url
                if after_url != before_url:
                    login_success = True
                    _create_status(new_run, "Captcha #1 solved successfully; logged in.")
                    break
            except Exception as e:
                logger.info("Login attempt %s failed: %s", attempt + 1, e)
                continue

        if not login_success:
            _create_status(new_run, "Login CAPTCHA solving failed after multiple attempts. Try again.")
            if driver:
                driver.quit()
            return JsonResponse({"message": "Login CAPTCHA solving failed after multiple attempts."}, status=500)

        # Navigate to search
        WebDriverWait(driver, DEFAULT_WAIT).until(EC.presence_of_element_located((By.CSS_SELECTOR, "h5.my-0")))
        search_certified = driver.find_elements(By.CSS_SELECTOR, "li.ng-star-inserted>a")
        if len(search_certified) > 2:
            driver.execute_script("arguments[0].click();", search_certified[2])
        else:
            if driver:
                driver.quit()
            return JsonResponse({"message": "Scraping failed: Initial elements not found."}, status=500)

        WebDriverWait(driver, DEFAULT_WAIT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.apex-item-option"))
        )

        # CAPTCHA #2: fill filters first
        captcha2_success = False
        for retry in range(max_attempts):
            try:
                driver.refresh()
                WebDriverWait(driver, DEFAULT_WAIT).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.apex-item-option"))
                )

                other_details = driver.find_elements(By.CSS_SELECTOR, "div.apex-item-option")
                if len(other_details) > 2:
                    driver.execute_script("arguments[0].click();", other_details[2])
                else:
                    if driver:
                        driver.quit()
                    return JsonResponse({"message": "Scraping failed: Other details elements not found."}, status=500)

                WebDriverWait(driver, DEFAULT_WAIT).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "input#P2000_FROM_DATE"))
                )

                _create_status(new_run, "Filling District, Date, and Deed Type")

                period_from = driver.find_element(By.CSS_SELECTOR, "input#P2000_FROM_DATE")
                period_from.click()
                period_from.clear()
                period_from.send_keys(date_from_fmt)

                period_to = driver.find_element(By.CSS_SELECTOR, "input#P2000_TO_DATE")
                period_to.click()
                period_to.clear()
                period_to.send_keys(date_to_fmt)

                WebDriverWait(driver, DEFAULT_WAIT).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "select#P2000_DISTRICT"))
                )
                element = driver.find_element(By.CSS_SELECTOR, "select#P2000_DISTRICT")
                Select(element).select_by_visible_text(district)

                # deed type autocomplete
                time.sleep(1.0)
                input_box = driver.find_element(By.XPATH, "//input[@aria-autocomplete='list']")
                input_box.clear()
                input_box.send_keys(deed_type)
                time.sleep(0.8)
                input_box.send_keys(Keys.ENTER)

                # CAPTCHA #2 image and solve
                captcha_imgs = driver.find_elements(By.CSS_SELECTOR, "div.input-group>img")
                if len(captcha_imgs) < 2:
                    raise RuntimeError("CAPTCHA #2 image not found.")
                captcha_img_el = captcha_imgs[1]

                captcha_img_2 = _screenshot_element(driver, captcha_img_el)
                _create_status(new_run, "Please solve CAPTCHA #2 in the UI", pil_image=captcha_img_2)

                captcha_value_2 = _wait_for_captcha_value(new_run.id, timeout=CAPTCHA_WAIT_SECONDS)
                if not captcha_value_2:
                    _create_status(new_run, "CAPTCHA #2 timed out waiting for input. Retrying...")
                    continue

                captcha_inputs = driver.find_elements(By.CSS_SELECTOR, "div.input-group>input")
                if len(captcha_inputs) < 2:
                    raise RuntimeError("CAPTCHA #2 input not found.")

                captcha_inputs[1].click()
                captcha_inputs[1].clear()
                captcha_inputs[1].send_keys(captcha_value_2)

                # Search
                search_button = driver.find_elements(By.CSS_SELECTOR, "div>button.btn")
                if len(search_button) >= 5:
                    driver.execute_script("arguments[0].click();", search_button[4])
                else:
                    raise RuntimeError("Search button not found.")

                # Wait for search results to load
                WebDriverWait(driver, DEFAULT_WAIT).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "td.mat-cell>span.link"))
                )
                captcha2_success = True
                _create_status(new_run, "CAPTCHA #2 solved successfully.")
                break
            except Exception as e:
                logger.info("CAPTCHA #2 attempt %s failed: %s", retry + 1, e)
                continue

        if not captcha2_success:
            _create_status(new_run, "CAPTCHA #2 solving failed after multiple attempts. Try again.")
            if driver:
                driver.quit()
            return JsonResponse({"message": "CAPTCHA #2 solving failed after multiple attempts."}, status=500)

        # Process paginated results
        WebDriverWait(driver, DEFAULT_WAIT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "td.mat-cell>span.link"))
        )

        while True:
            data_elements = WebDriverWait(driver, DEFAULT_WAIT).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "td.mat-cell>span.link"))
            )

            for i in range(len(data_elements)):
                try:
                    data_elements_refresh = WebDriverWait(driver, DEFAULT_WAIT).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "td.mat-cell>span.link"))
                    )
                    if i >= len(data_elements_refresh):
                        break

                    span = data_elements_refresh[i]
                    driver.execute_script("arguments[0].click();", span)
                    WebDriverWait(driver, DEFAULT_WAIT).until(
                        EC.presence_of_element_located((By.XPATH, "//fieldset[legend[contains(text(), 'Registration Details')]]"))
                    )

                    _create_status(new_run, f"Fetching data of record index {i}")

                    # Registration Details
                    registration_data = driver.find_elements(
                        By.XPATH, "//fieldset[legend[contains(text(), 'Registration Details')]]/div/table/tbody/tr/td"
                    )
                    registration_heading = driver.find_elements(
                        By.XPATH, "//fieldset[legend[contains(text(), 'Registration Details')]]/div/table/thead/tr/th"
                    )
                    headings = [th.text.strip() for th in registration_heading]
                    data_texts = [td.text.strip() for td in registration_data]

                    # Seller
                    seller_data = driver.find_elements(
                        By.XPATH, '//fieldset[legend[contains(text(), "Party From")]]/div/table/tbody/tr/td'
                    )
                    seller_heading = driver.find_elements(
                        By.XPATH, "//fieldset[legend[contains(text(), 'Party From')]]/div/table/thead/tr/th"
                    )
                    headings_2 = [th.text.strip() for th in seller_heading]
                    data_texts_2 = [td.text.strip() for td in seller_data]

                    # Buyer
                    buyer_data = driver.find_elements(
                        By.XPATH, "//fieldset[legend[contains(text(), 'Party To')]]/div/table/tbody/tr/td"
                    )
                    buyer_heading = driver.find_elements(
                        By.XPATH, "//fieldset[legend[contains(text(), 'Party To')]]/div/table/thead/tr/th"
                    )
                    headings_3 = [th.text.strip() for th in buyer_heading]
                    data_texts_3 = [td.text.strip() for td in buyer_data]

                    # Property Details
                    property_details = driver.find_elements(
                        By.XPATH, "//fieldset[legend[contains(text(), 'Property Details')]]/div/table/tbody/tr/td"
                    )
                    property_heading = driver.find_elements(
                        By.XPATH, "//fieldset[legend[contains(text(), 'Property Details')]]/div/table/thead/tr/th"
                    )
                    headings_4 = [th.text.strip() for th in property_heading]
                    data_texts_4 = [td.text.strip() for td in property_details]

                    # Khasra/Building/Plot Details
                    khasra_building_plot_details = driver.find_elements(
                        By.XPATH, "//fieldset[legend[contains(text(), 'Khasra/Building/Plot Details')]]/div/table/tbody/tr/td"
                    )
                    khasra_heading = driver.find_elements(
                        By.XPATH, "//fieldset[legend[contains(text(), 'Khasra/Building/Plot Details')]]/div/table/thead/tr/th"
                    )
                    headings_5 = [th.text.strip() for th in khasra_heading]
                    data_texts_5 = [td.text.strip() for td in khasra_building_plot_details]

                    # Parse address inside property details (augment with parsed fields)
                    final_data_texts_4 = []
                    for heading_100, data_val in zip(headings_4, data_texts_4):
                        if "address" in heading_100.lower():
                            parsed_addr = parse_address(data_val)
                            for k, v in parsed_addr.items():
                                final_data_texts_4.append((k, v))
                        else:
                            final_data_texts_4.append((heading_100, data_val))

                    headings_4_parsed = [h for h, _ in final_data_texts_4]
                    data_texts_4_parsed = [v for _, v in final_data_texts_4]

                    all_sections = [
                        (headings, data_texts),
                        (headings_2, data_texts_2),
                        (headings_3, data_texts_3),
                        (headings_4_parsed, data_texts_4_parsed),
                        (headings_5, data_texts_5),
                    ]

                    # Save to DB
                    save_to_db(all_sections)

                    # Close popup
                    try:
                        close_buttons = WebDriverWait(driver, DEFAULT_WAIT).until(
                            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "button.colsebtn"))
                        )
                        btn = close_buttons[1] if len(close_buttons) > 1 else close_buttons[0]
                        driver.execute_script("arguments[0].click();", btn)
                        time.sleep(0.5)
                    except Exception:
                        logger.info("Close button not found or could not be clicked")
                except Exception as e:
                    logger.info("Failed to process record index %s: %s", i, e)
                    # Attempt to proceed to next item
                    try:
                        # If modal is open and failed, try to close
                        close_buttons = driver.find_elements(By.CSS_SELECTOR, "button.colsebtn")
                        if close_buttons:
                            driver.execute_script("arguments[0].click();", close_buttons[0])
                    except Exception:
                        pass
                    continue

            # Pagination
            try:
                next_button = driver.find_element(By.CSS_SELECTOR, "button.mat-paginator-navigation-next")
                classes = next_button.get_attribute("class") or ""
                if "disabled" in classes:
                    break
                driver.execute_script("arguments[0].click();", next_button)
                WebDriverWait(driver, DEFAULT_WAIT).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "td.mat-cell>span.link"))
                )
            except Exception:
                break

        _create_status(
            new_run,
            "Scraping completed successfully! Go to /get-status/ to review and download from /download-excel/",
        )
        return JsonResponse({"message": f"Scraping completed successfully! {timestamp_2}"})

    except Exception as e:
        logger.exception("Error occurred during scraping: %s", e)
        _create_status(new_run, "Scraping failed due to an error. Please check logs and try again.")
        return JsonResponse({"message": f"Scraping failed: {e}"}, status=500)
    finally:
        if driver:
            driver.quit()


def clear_logs(request):
    ScrapingStatus.objects.all().delete()
    return JsonResponse({"message": "Logs cleared"})


def download_excel(request):
    """
    Export ScrapedRecord to Excel. Handles None JSON fields gracefully.
    """
    records = ScrapedRecord.objects.all()
    wb = Workbook()
    ws = wb.active

    if records.exists():
        first = records.first()
        registration = first.registration_details or {}
        seller = first.seller_details or {}
        buyer = first.buyer_details or {}
        prop = first.property_details or {}
        khasra = first.khasra_details or {}

        headers = list(registration.keys()) + list(seller.keys()) + list(buyer.keys()) + list(prop.keys()) + list(khasra.keys())
        ws.append(headers)

        for r in records:
            row = list((r.registration_details or {}).values()) + \
                  list((r.seller_details or {}).values()) + \
                  list((r.buyer_details or {}).values()) + \
                  list((r.property_details or {}).values()) + \
                  list((r.khasra_details or {}).values())
            ws.append(row)
    else:
        ws.append(["No data"])

    response = HttpResponse(
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = 'attachment; filename="scraped_data.xlsx"'
    wb.save(response)
    return response