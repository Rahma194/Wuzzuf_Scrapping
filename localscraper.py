import requests
from dotenv import load_dotenv
import os 
import json
from config import BASE_URL, PROGRESS_FILE,OUTPUT_DIR,OUTPUT_FILE,MAX_RETRIES,RETRY_DELAY,LISTING_PATH,TOTAL_PAGES,BATCH_SIZE,CONCURRENCY
import csv
import asyncio
from playwright.async_api import async_playwright
import time


load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")


#send telegram messages and files

def send_telegram_message(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, data={"chat_id": CHAT_ID, "text": msg})
    except Exception as e:
        print("Telegram error:", e)

def send_telegram_file(file_path):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
    try:
        with open(file_path, "rb") as f:
            requests.post(url, data={"chat_id": CHAT_ID}, files={"document": f})
    except Exception as e:
        print("Telegram file error:", e)

#-----------------------------------------------------------------------------------------

#progress tracking functions

def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"last_page": 0, "last_link_index": -1, "batch_count": 0}

def save_progress(page_number, link_index, batch_count):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(
            {
                "last_page": page_number,
                "last_link_index": link_index,
                "batch_count": batch_count,
            }, 
            f,
        )

def clear_progress():
    if os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)

#-----------------------------------------------------------------------------------------

#flatten the nested job dict into a single level dict for csv writing

def flatten_job(job : dict) -> dict:
    flat_job = {
        "title": job.get("title"),
        "company_name": job.get("company_name"),
        "location": job.get("location"),
        "job_type": job.get("job_type"),
        "workplace": job.get("workplace"),
    }
    
    # Flatten details
    for key, value in job.get("details", {}).items():
        flat_job[f"detail_{key}"] = value
    
    # Flatten skills
    skills = job.get("skills", [])
    flat_job["skill"] = ", ".join(skills)
    
    return flat_job

#-----------------------------------------------------------------------------------------

#write a batch of job data to a csv file 

def write_csv_batch(batch_data: list[dict], batch_number: int) -> str:
    flat_rows = [flatten_job(job) for job in batch_data]
    all_columns = list(dict.fromkeys(col for row in flat_rows for col in row.keys()))

    file_path = os.path.join(OUTPUT_DIR, f"batch_{batch_number}.csv")
    with open(file_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(flat_rows)

    return file_path

#-----------------------------------------------------------------------------------------


def merge_batches_to_final():
    batch_files = sorted(
        [
            os.path.join(OUTPUT_DIR, f)
            for f in os.listdir(OUTPUT_DIR)
            if f.startswith("batch_") and f.endswith(".csv")
        ]
    )
    if not batch_files:
        return
    
    all_columns = []
    for bf in batch_files:
        with open(bf, "r" , encoding= "utf-8-sig") as f:
            reader = csv.DictReader(f)
            for col in reader.fieldnames or []:
                if col not in all_columns:
                    all_columns.append(col)

    final_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    with open(final_path, "w", encoding="utf-8-sig", newline="") as out:
        writer = csv.DictWriter(out, fieldnames=all_columns, extrasaction="ignore")
        writer.writeheader()
        for bf in batch_files:
            with open(bf, "r", encoding="utf-8-sig") as batch_f:
                reader = csv.DictReader(batch_f)
                for row in reader:
                    writer.writerow(row)
        
    for bf in batch_files:
        os.remove(bf)

    print(f"Final csv saved -> {final_path}")

#--------------------------------------------------------------------------
#scrape the listing page to get the job links

async def scrape_page_links(page, page_number):
    for attempt in range(1,MAX_RETRIES + 1):
        try:
            await page.goto(
                f"{BASE_URL}{LISTING_PATH.format(test_page_number=page_number)}",
                wait_until = "domcontentloaded",
                timeout = 30000,
            )
            await page.wait_for_timeout(5000)

            links = await page.locator("a.css-o171kl").evaluate_all(
                "elements => elements.map(el => el.getAttribute('href'))")
            
            valid_links = list(dict.fromkeys([
                l for l in links if l and "/a/" not in l
            ]))
            return valid_links
        except Exception as e:
            print(f"Page {page_number} attempt {attempt}/{MAX_RETRIES} failed:{e}")
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)
            else:
                print(f"skipping page {page_number} after {MAX_RETRIES} retries")
                return []
            
#------------------------------------------------------------------------------------------------
#scrape the job details page to extract the required information

async def scrape_job_details(context , url, semaphore):
    async with semaphore:
        page = await context.new_page()
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                await page.goto(f"{BASE_URL}{url}", 
                                wait_until="domcontentloaded", 
                                timeout=30000)
                await page.wait_for_timeout(5000)
                overview = page.locator("#app")
                title = (await overview.locator("h1").inner_text()).strip()

                specs_1 = await overview.locator("span.eoyjyou0").all_inner_texts()
                Job_type = specs_1[0] if len(specs_1) > 0 else None
                Workplace = specs_1[1] if len(specs_1) > 1 else None

                company_name = (await overview.locator("a.css-p7pghv").first.inner_text()).strip()

                #location
                containers = await page.locator(".css-1vlp604").all()
                for container in containers:
                    try:
                        full_text = await container.inner_text()
                        location = full_text.split("-")[-1].strip()    
                    except Exception:
                        print("Could not extract location for this card")


                #details
                details = page.locator(".css-1ajx53j")
                data = {}
                for detail in  await details.all():
                    label = detail.locator(".css-720fa0")
                    value = detail.locator(".css-iu2m7n")
                    if await label.count() > 0 and await value.count() > 0:
                        label = (await label.inner_text()).strip()
                        value = (await value.inner_text()).strip()
                        data[label] = value

                #skills
                skills_container = page.locator(".css-qe7mba")
                skills_list = await skills_container.locator("a").all_text_contents()
                skills = [skill.strip() for skill in skills_list]


                return {
                    "title": title,
                    "company_name": company_name,
                    "job_type": Job_type,
                    "workplace": Workplace,
                    "location": location,
                    "details": data,
                    "skills": skills,
                }
            except Exception as e:
                print(f"Error scraping {url} attempt {attempt}/{MAX_RETRIES}: {e}")
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(RETRY_DELAY)
                else:
                    print(f"skipping {url} after {MAX_RETRIES} retries")
                    return None
            finally:
                pass
        await page.close()

#-----------------------------------------------------------------------------------------

async def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    start_time = time.time()
    progress = load_progress()
    start_page = progress["last_page"] if progress["last_page"] > 0 else 1
    skip_links_before = progress["last_link_index"] + 1 
    batch_count = progress["batch_count"]

    batch_data = []
    job_counter = 0
    current_page = start_page
    current_chunk_idx = 0

    is_running = progress["last_page"] > 0
    if is_running:
        send_telegram_message(
            f"Resuming scraping from page {start_page},"
            f"link index {skip_links_before}, batch {batch_count + 1}"
            )
    else:
        send_telegram_message("scraping started")

    async with async_playwright() as p:
        try:
            for page_number in range(start_page, TOTAL_PAGES + 1):
                current_page = page_number
                print(f"scraping page {page_number}/{TOTAL_PAGES}")

                list_browser = await p.chromium.launch(headless=False)
                list_context = await list_browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                    viewport={"width": 1280, "height": 720},
                )
                list_page = await list_context.new_page()
                links = await scrape_page_links(list_page, page_number)
                await list_browser.close()

                for i in range(0,len(links),BATCH_SIZE):
                    current_chunk_idx = i
                    
                    if page_number == start_page and i < skip_links_before:
                        continue
                    chunk_links = links[i:i + BATCH_SIZE]
                    print("processing batch of {len(chunk_links)} links concurrently...")

                    chunk_browser = await p.chromium.launch(headless=False)
                    chunk_context = await chunk_browser.new_context(
                        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                        viewport={"width": 1280, "height": 720},
                    )
                    semaphore = asyncio.Semaphore(CONCURRENCY)
                    tasks = [
                        scrape_job_details(chunk_context, link, semaphore) 
                        for link in chunk_links]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    await chunk_browser.close()
                    for res in results:
                        if isinstance(res, dict) and res is not None:
                            batch_data.append(res)
                            job_counter += 1
                        elif isinstance(res, Exception):
                            print(f"Exception during gathering: {res}")
                    if batch_data:
                        batch_count += 1
                        file_path = write_csv_batch(batch_data, batch_count)
                        send_telegram_file(file_path)
                        print(f"Batch {batch_count} sent ({len(batch_data)} jobs)")
 
                        save_progress(page_number, i + len(chunk_links) - 1, batch_count)
                        batch_data = []
                if batch_data:
                    batch_count += 1
                    file_path = write_csv_batch(batch_data, batch_count)
                    send_telegram_file(file_path)
                    print(f"Batch {batch_count} sent ({len(batch_data)} jobs)")

                merge_batches_to_final()
                clear_progress()
                end_time = time.time()
                duration = end_time - start_time
                send_telegram_message(
                    f"Scraping completed!\n"
                    f"Jobs scraped: {job_counter}\n"
                    f"Batches sent: {batch_count}\n"
                    f"Duration: {duration:.2f}s\n"
                    f"Final file: {OUTPUT_FILE}"
                )
        except Exception as e:
            if batch_data:
                batch_count += 1
                file_path = write_csv_batch(batch_data, batch_count)
                send_telegram_file(file_path)
 
            save_progress(current_page, current_chunk_idx, batch_count)
            print(f"Saved progress at page {current_page}, chunk offset {current_chunk_idx} before exit")
            send_telegram_message(
                f"Scraping crashed: {e}\nRun again to resume.")
            try:
                if 'chunk_browser' in locals() and chunk_browser:
                    await chunk_browser.close()
                if 'list_browser' in locals() and list_browser:
                    await list_browser.close()
            except Exception:
                pass
            raise            
 
if __name__ == "__main__":
    asyncio.run(main())