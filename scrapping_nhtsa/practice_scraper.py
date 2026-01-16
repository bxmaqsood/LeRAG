import asyncio
from playwright.async_api import async_playwright
from dotenv import load_dotenv
import os
import re
import json
import ast

# Load .env variables
load_dotenv()
YEAR = os.getenv("YEAR")
YEARS = ast.literal_eval(YEAR)

def push_data_to_airtable(nhtsa_data, year):
    print(f"Ready to push {len(nhtsa_data)} records for {year} to Airtable.")


def save_partial_results(year, complaints):
    file_path = f"nhtsa_complaints_{year}_partial.json"
    with open(file_path, "w") as f:
        json.dump(complaints, f, indent=2)
    print(f"💾 Saved partial results ({len(complaints)}) for {year} to {file_path} due to error.")



def clean_summary(text):
    if not text:
        return ""

    # Remove known prefix patterns
    text = re.sub(r'CrashNo\s*FireNo\s*Injuries\d+\s*Deaths\d+', '', text, flags=re.IGNORECASE)

    # Remove trailing junk like "1 Affected Product"
    text = re.sub(r'\d+\s+Affected Product.*$', '', text, flags=re.IGNORECASE)

    # Normalize whitespace and newlines
    text = re.sub(r'\s+', ' ', text).strip()

    return text


async def extract_complaints_data(page):
    await page.wait_for_selector('.vehicle-issues.panel-group')

    try:
        small_element = await page.query_selector("section#complaints h1 > small")
        if small_element:
            small_text = await small_element.inner_text()
            clean_text = re.sub(r'^\s*for\s+', '', small_text, flags=re.IGNORECASE).strip()
            match = re.match(r"(\d{4})\s+(.+)", clean_text)
            if match:
                extracted_year = match.group(1)
                vehicle_name = match.group(2).strip()
            else:
                extracted_year = ""
                vehicle_name = ""
        else:
            extracted_year = ""
            vehicle_name = ""
    except Exception as e:
        print("❌ Failed to extract year and vehicle name:", e)
        extracted_year = ""
        vehicle_name = ""

    complaint_panels = await page.query_selector_all('.panel-item')
    complaints = []

    for panel in complaint_panels:
        data = {
            "year": extracted_year,
            "vehicle": vehicle_name
        }
        try:
            date_div = await panel.query_selector('div.panel-title div')
            date_text = await date_div.inner_text()
            if 'NHTSA ID NUMBER:' in date_text:
                parts = date_text.split('NHTSA ID NUMBER:')
                data['reportedDate'] = parts[0].strip()
                data['nhtsaId'] = parts[1].strip()
            else:
                data['reportedDate'] = ''
                data['nhtsaId'] = ''

            component_div = await panel.query_selector('span.panel-title-caption')
            if component_div:
                component_text = await component_div.inner_text()
                data['component'] = re.sub(r'^\s*Components?:\s+', '', component_text, flags=re.IGNORECASE).strip()
            else:
                data['component'] = ''

            body = await panel.query_selector('.panel-body')

            async def extract_field(label):
                p = body and await body.query_selector(f"p:has-text('{label}')")
                if p:
                    span = await p.query_selector('span')
                    return await span.inner_text() if span else ''
                return ''

            data['IncidentDate'] = await extract_field("Incident Date")
            data['consumerLocation'] = await extract_field("Consumer Location")
            data['vin'] = await extract_field("Vehicle Identification Number")

            summary = await body.query_selector('p.vehicle-detail--issue-summary')
            if summary:
                summary_p = await summary.evaluate_handle("el => el.nextElementSibling")
                raw_summary = await summary_p.inner_text() if summary_p else ''
                data['summary'] = clean_summary(raw_summary)

            else:
                data['summary'] = ''

            stats_col = await body.query_selector('.vehicle-details--complaints-sidebar')
            if stats_col:
                stats = await stats_col.query_selector_all('p')
                for stat in stats:
                    spans = await stat.query_selector_all('span')
                    if len(spans) == 2:
                        key = await spans[0].inner_text()
                        val = await spans[1].inner_text()
                        if key.lower() == 'crash':
                            data['crash'] = val
                        elif key.lower() == 'fire':
                            data['fire'] = val
                        elif 'injuries' in key.lower():
                            data['injuries'] = val
                        elif 'deaths' in key.lower():
                            data['deaths'] = val

            complaints.append(data)

        except Exception as e:
            print("Error parsing panel:", e)

    return complaints


async def scrape_year(year, context):
    page = await context.new_page()
    await page.goto("https://www.nhtsa.gov/recalls")
    await page.fill('#ymm-vin-recalls-search-input', year)
    await page.keyboard.press('Enter')
    await page.wait_for_timeout(3000)

    for attempt in range(3):
        try:
            await page.wait_for_selector("div[role='button'].cansort.unsorted", timeout=5000)
            button_complaints = await page.query_selector("div[role='button'].cansort.unsorted:has-text('Complaints')")
            if button_complaints:
                await button_complaints.scroll_into_view_if_needed()
                await button_complaints.click()
                await page.wait_for_timeout(1000)

                button_complaints = await page.query_selector("div[role='button'].cansort:has-text('Complaints')")
                if button_complaints:
                    await button_complaints.scroll_into_view_if_needed()
                    await button_complaints.click()
                    await page.wait_for_timeout(1000)

                print(f"✅ Double click successful on Complaints for {year}.")
                break
            else:
                print(f"❌ Complaints button not found for {year}.")

        except Exception as e:
            print(f"Attempt {attempt+1} for {year} failed:", e)
            await page.wait_for_timeout(1000)

    final_complaints = []
    MAX_COMPLAINTS = 1000

    while len(final_complaints) < MAX_COMPLAINTS:
        print(f"Processing page for year {year}...")

        await page.wait_for_selector("table")
        rows = await page.query_selector_all("table > tbody > tr")

        for row in rows:
            if len(final_complaints) >= MAX_COMPLAINTS:
                break

            links = await row.query_selector_all("td a")
            if len(links) >= 2:
                investigation_link = await links[2].get_attribute("href")
                if investigation_link:
                    full_url = "https://www.nhtsa.gov" + investigation_link
                    print("Opening:", full_url)

                    new_page = await context.new_page()
                    await new_page.goto(full_url)

                    try:
                        # Loop through multiple pages in investigation view
                        while len(final_complaints) < MAX_COMPLAINTS:
                            complaints = await extract_complaints_data(new_page)
                            needed = MAX_COMPLAINTS - len(final_complaints)
                            records_to_add = complaints[:min(5, needed)]
                            final_complaints.extend(records_to_add)
                            print(f"✅ Added {len(records_to_add)} complaints. Total now: {len(final_complaints)}")

                            if len(final_complaints) >= MAX_COMPLAINTS:
                                break

                            next_button = await new_page.query_selector("button.link-arrow:has-text('next')")
                            if next_button and await next_button.is_enabled():
                                await next_button.click()
                                await new_page.wait_for_timeout(1000)
                            else:
                                break

                    except Exception as e:
                        print("Error scraping complaints:", e)
                        save_partial_results(year, final_complaints)


                    await new_page.close()

        if len(final_complaints) >= MAX_COMPLAINTS:
            break

        next_button = await page.query_selector("button.link-arrow:has-text('next')")
        if next_button and await next_button.is_enabled():
            await next_button.click()
            await page.wait_for_timeout(1000)
        else:
            break

    await page.close()

    with open(f"nhtsa_complaints_{year}.json", "w") as f:
        json.dump(final_complaints, f, indent=2)

    print(f"\n✅ Saved {len(final_complaints)} complaints for {year}.")
    # push_data_to_airtable(final_complaints, year)


async def scrape_all_years():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, slow_mo=200)
        context = await browser.new_context()

        for year in YEARS:
            try:
                await scrape_year(year, context)
            except Exception as e:
                print(f"Error scraping {year}:", e)

        await browser.close()

# Run
asyncio.run(scrape_all_years())
