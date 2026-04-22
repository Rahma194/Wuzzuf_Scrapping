from playwright.sync_api import sync_playwright
from config import BASE_URL,test_page_number

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    page = browser.new_page()
    page.goto(f"{BASE_URL}/search/jobs?start={test_page_number}")
    page.wait_for_timeout(5000)
    

    links = page.locator("a.css-o171kl").evaluate_all(
        "elements => elements.map(el => el.getAttribute('href'))")
    
    valid_links = list(dict.fromkeys([
        l for l in links if l and "/a/" not in l
    ]))
    
    page.goto(f"{BASE_URL}{valid_links[5]}")
    page.wait_for_timeout(5000)

    #title
    overview = page.locator("#app")
    title = overview.locator("h1").inner_text().strip()

    #job type and workplace
    specs_1 = overview.locator("span.eoyjyou0").all_inner_texts()
    Job_type = specs_1[0]
    Workplace = specs_1[1]
    
    #company name
    company_name = overview.locator("a.css-p7pghv").first.inner_text()

    #location
    containers = page.locator(".css-1vlp604").all()
    for container in containers:
        try:
            full_text = container.inner_text()
            location = full_text.split("-")[-1].strip()    
        except Exception:
            print("Could not extract location for this card")


    #details
    details = page.locator(".css-1ajx53j")
    data = {}
    for detail in details.all():
        label = detail.locator(".css-720fa0")
        value = detail.locator(".css-iu2m7n")
        if label.count() > 0 and value.count() > 0:
            label = label.inner_text().strip()
            value = value.inner_text().strip()
            data[label] = value

    #skills
    skills_container = page.locator(".css-qe7mba")
    skills_list = skills_container.locator("a").all_text_contents()
    skills = [skill.strip() for skill in skills_list]
    

    
    print({
        "title": title,
        "company_name": company_name,
        "job_type": Job_type,
        "workplace": Workplace,
        "location": location,
        "details": data,
        "skills": skills,
})


    browser.close()