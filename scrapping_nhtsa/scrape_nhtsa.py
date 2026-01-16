import asyncio
from playwright.async_api import async_playwright
from dotenv import load_dotenv
import os
import json

# Load environment variables
load_dotenv()
YEAR = os.getenv("YEAR")

async def extract_complaints_data(page, max_to_collect, collected_so_far):
    complaints = []

    while True:
        await page.wait_for_selector('.vehicle-issues.panel-group')
        complaint_panels = await page.query_selector_all('.panel-item')

        for panel in complaint_panels:
            if len(complaints) + collected_so_far >= max_to_collect:
                return complaints

            data = {}

            try:
                # Top Section: Date + NHTSA ID
                date_div = await panel.query_selector('div.panel-title div')
                date_text = await date_div.inner_text()
                if 'NHTSA ID NUMBER:' in date_text:
                    parts = date_text.split('NHTSA ID NUMBER:')
                    data['incidentDate'] = parts[0].strip()
                    data['nhtsaId'] = parts[1].strip()
                else:
                    data['incidentDate'] = ''
                    data['nhtsaId'] = ''

                # Body Section
                body = await panel.query_selector('.panel-body')

                async def extract_field(label):
                    p = body and await body.query_selector(f"p:has-text('{label}')")
                    if p:
                        span = await p.query_selector('span')
                        return await span.inner_text() if span else ''
                    return ''

                data['consumerLocation'] = await extract_field("Consumer Location")
                data['vin'] = await extract_field("Vehicle Identification Number")

                summary = await body.query_selector('p.vehicle-detail--issue-summary')
                if summary:
                    summary_p = await summary.evaluate_handle("el => el.nextElementSibling")
                    data['summary'] = await summary_p.inner_text() if summary_p else ''
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

        # ✅ Pagination logic for complaints page
        next_button = await page.query_selector("button.link-arrow:has-text('next')")
        if next_button and await next_button.is_enabled():
            await next_button.click()
            await page.wait_for_timeout(1500)
        else:
            break

    return complaints


async def scrape_investigations():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, slow_mo=200)
        context = await browser.new_context()
        page = await context.new_page()

        await page.goto("https://www.nhtsa.gov/recalls")
        await page.fill('#ymm-vin-recalls-search-input', YEAR)
        await page.keyboard.press('Enter')
        await page.wait_for_timeout(2000)

        all_complaints = []
        MAX_COMPLAINTS = 20


        while True:
            print("Processing current page...")

            await page.wait_for_selector("table")
            rows = await page.query_selector_all("table > tbody > tr")

            for row in rows:
                links = await row.query_selector_all("td a")
                if len(links) >= 2:
                    investigation_link = await links[1].get_attribute("href")
                    if investigation_link:
                        full_url = "https://www.nhtsa.gov" + investigation_link
                        print("Opening:", full_url)

                        new_page = await context.new_page()
                        await new_page.goto(full_url)
                        await page.wait_for_timeout(100)
                        await new_page.close()


                    

                        # # inside loop over result rows:
                        # try:
                        #     complaints = await extract_complaints_data(new_page, MAX_COMPLAINTS, len(all_complaints))
                        #     all_complaints.extend(complaints)
                        #     print(f"Total complaints collected: {len(all_complaints)}")
                        # except Exception as e:
                        #     print("Error scraping complaints:", e)

                        # await new_page.close()

                        # if len(all_complaints) >= MAX_COMPLAINTS:
                        #     all_complaints = all_complaints[:MAX_COMPLAINTS]
                        #     with open("nhtsa_complaints.json", "w") as f:
                        #         json.dump(all_complaints, f, indent=2)
                        #     print(f"\n✅ Saved {MAX_COMPLAINTS} complaints to nhtsa_complaints.json")
                        #     await browser.close()
                        #     return


            # Handle pagination
            next_button = await page.query_selector("button.link-arrow:has-text('next')")
            if next_button and await next_button.is_enabled():
                await next_button.click()
                await page.wait_for_timeout(1000)
            else:
                break

        await browser.close()

        # # Save whatever was collected (less than 10)
        # with open("nhtsa_complaints.json", "w") as f:
        #     json.dump(all_complaints[:10], f, indent=2)
        # print("\n✅ Saved", len(all_complaints[:10]), "complaints to nhtsa_complaints.json")

# Run it
asyncio.run(scrape_investigations())
