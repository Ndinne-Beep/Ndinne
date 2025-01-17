import asyncio
from playwright.async_api import async_playwright

import pandas as pd
from io import BytesIO
from azure.storage.blob import BlobClient
from datetime import datetime, timedelta
import re
from bs4 import BeautifulSoup


all_data = []
blocked_days = []
unblocked_days_final = []

failed_ids=[]
unavailable_ids=[]



def get_urls():
    sas_url = f"https://privateproperty.blob.core.windows.net/airbnb/Listingids.csv?sv=2023-01-03&st=2025-01-13T07%3A50%3A48Z&se=2027-01-14T07%3A50%3A00Z&sr=c&sp=racwdxltf&sig=EUqQibbbiaeiTEE7fX%2FK8SS5j6%2Fao%2F%2BtmrS5z2Tzbm4%3D"
    client = BlobClient.from_blob_url(sas_url)
    blob = client.download_blob()

    blob_content = blob.readall()
    data = BytesIO(blob_content)
    data1 = pd.read_csv(data)

    # Select the first column for the specified range of rows
    selected_rows = data1.iloc[:, 0]
    return selected_rows


def ingest_data(all_data):
    start_time = time.time()
    if all_data:
        # Convert data to DataFrame
        data_frame = pd.DataFrame(all_data)
        # Convert DataFrame to CSV as binary data
        car_data_csv = data_frame.to_csv(encoding="utf-8", index=False).encode("utf-8")
        # SAS URL for uploading
        sas_url = f"https://privateproperty.blob.core.windows.net/airbnb/AirbnbListings{start_time}.csv?sv=2023-01-03&st=2025-01-13T07%3A50%3A48Z&se=2027-01-14T07%3A50%3A00Z&sr=c&sp=racwdxltf&sig=EUqQibbbiaeiTEE7fX%2FK8SS5j6%2Fao%2F%2BtmrS5z2Tzbm4%3D"
        # Create BlobClient
        client = BlobClient.from_blob_url(sas_url)
        # Upload blob
        client.upload_blob(car_data_csv, overwrite=True)
        print("Data uploaded successfully.")
    else:
        print("No data to ingest.")
        
def ingest_removed_id(unavailable_ids):
    start_time = time.time()
    if unavailable_ids:
        data_frame = pd.DataFrame(unavailable_ids)
        car_data_csv = data_frame.to_csv(encoding="utf-8", index=False).encode("utf-8")
        sas_url = f"https://privateproperty.blob.core.windows.net/airbnb/Air_removed{start_time}.csv?sv=2023-01-03&st=2025-01-13T07%3A50%3A48Z&se=2027-01-14T07%3A50%3A00Z&sr=c&sp=racwdxltf&sig=EUqQibbbiaeiTEE7fX%2FK8SS5j6%2Fao%2F%2BtmrS5z2Tzbm4%3D"
        # Create BlobClient
        client = BlobClient.from_blob_url(sas_url)
        # Upload blob
        client.upload_blob(car_data_csv, overwrite=True)
        print("Data uploaded successfully.")
    else:
        print("No data to ingest.")        


def generate_check_dates():
    current_date = datetime.now()
    start_year = current_date.year
    start_month = (current_date.month % 12) + 1
    start_day = current_date.day

    # Generate check-in dates
    check_in_dates = [
        datetime(start_year + (start_month - 1) // 12, (start_month - 1) % 12 + 1, start_day),
        datetime(start_year + (start_month + 2 - 1) // 12, (start_month + 2 - 1) % 12 + 1, 1),
        #datetime(start_year + (start_month + 5 - 1) // 12, (start_month + 5 - 1) % 12 + 1, 1),
        #datetime(start_year + (start_month + 8 - 1) // 12, (start_month + 8 - 1) % 12 + 1, 1),
        #datetime(start_year + (start_month + 11 - 1) // 12, (start_month + 11 - 1) % 12 + 1, 1),
    ]

    check_out_dates = [date + timedelta(days=2) for date in check_in_dates]
    
    return check_in_dates, check_out_dates

def convert_date_string(date_str):
    if isinstance(date_str, datetime):
        return date_str
    return datetime.strptime(date_str, '%m/%d/%Y')


def clean_text(text):
    text = re.sub(r'[\r\n\t]', ' ', text)
    text = re.sub(r'Â·', '', text)
    text = re.sub(r'\s+', ' ', text)
    text = text.strip() 
    return text

def extract_listing_data(soup, id):
    data = {}

    listingid = id
    data['listingid'] = listingid

    h1_tag = soup.find('h1')
    if h1_tag:
        data['title'] = clean_text(h1_tag.text)

    h2_tag = soup.find('h2')
    if h2_tag:
        data['locality'] = clean_text(h2_tag.text)

    desc_list = soup.find('ol').find_all('li')
    listing_features = " ".join(clean_text(i.text) for i in desc_list)
    data['listing_features'] = listing_features

    host_tag = soup.find('div', class_='t1pxe1a4')
    if host_tag:
        data['hostname'] = clean_text(host_tag.text)

    host_details = soup.find('div', class_='s1l7gi0l')
    if host_details:
        data['years_hosting'] = clean_text(host_details.text)

    try:
        location_tag = soup.find('div', class_='_1t2xqmi').find('h3', class_='hpipapi')
        if location_tag:
            data['location'] = clean_text(location_tag.text)
        else:
            data['location'] = None
    except AttributeError:
        data['location'] = None

    try:
        price_divs = soup.find_all('div', class_='_tr4owt')
        for div in price_divs:
            if 'x' in div.text.strip():
                price_per_night_tag = div.find('div', class_='l1x1206l')
                if price_per_night_tag:
                    price_per_night = clean_text(price_per_night_tag.text)
                    price_per_night = price_per_night.split(' ')[0].replace('Â', '').replace('ZAR', '')
                    data['price_per_night'] = price_per_night
            elif 'Cleaning' in div.text.strip():
                cleaning_fee_tag = div.find('span', class_='_1k4xcdh')
                if cleaning_fee_tag:
                    cleaning_fee = clean_text(cleaning_fee_tag.text)
                    cleaning_fee = cleaning_fee.replace('Â', '').replace('ZAR', '')
                    data['cleaning_fee'] = cleaning_fee
            elif 'service' in div.text.strip():
                service_fee_tag = div.find('span', class_='_1k4xcdh')
                if service_fee_tag:
                    service_fee = clean_text(service_fee_tag.text)
                    service_fee = service_fee.replace('Â', '').replace('ZAR', '')
                    data['service_fee'] = service_fee
    except IndexError as e:
        print(f"Error accessing row data: {e}")

    unblocked_days = []
    blocked_days_dict = {}
    
    div_tags = soup.find_all('div', {'data-is-day-blocked': True})

    for div in div_tags:
        full_date = div.get('data-testid').replace('calendar-day-', '')
        is_blocked = div['data-is-day-blocked'] == 'true'

        if is_blocked:
            blocked_days_dict[full_date] = 'blocked'
        else:
            unblocked_days.append(full_date) 
            unblocked_days_final.extend(unblocked_days)  
    
    blocked_days = str(blocked_days_dict)
    data['blocked_days'] = blocked_days
    
    Timestamp = datetime.now().strftime('%Y-%m-%d')
    data["Timestamp"] = Timestamp

    return data

all_listing_ids_final = get_urls()
#all_listing_ids_final = all_listing_ids_final[:60]


# Function to divide the list into chunks of a specified size
def chunk_list(data, chunk_size):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

# Async function to scrape a single ID
async def scrape_id(playwright, id, check_in, check_out):
    check_in_str = check_in.strftime('%Y-%m-%d')
    check_out_str = check_out.strftime('%Y-%m-%d')
    url = f"https://www.airbnb.co.za/rooms/{id}?adults=1&category_tag=Tag%3A8678&enable_m3_private_room=true&photo_id=1602362717&search_mode=regular_search&source_impression_id=p3_1721650674_P36bK5Ps7ufayXNj&previous_page_section_name=1000&federated_search_id=9cc4e6a6-d201-4efc-b0a6-142e51c608da&guests=1&check_in={check_in_str}&check_out={check_out_str}"
    
    #url = f"https://www.airbnb.co.za/rooms/{id}"
    browser = await playwright.chromium.launch(headless=False)
    page = await browser.new_page()
    try:
        await page.goto(url)
        await asyncio.sleep(1)
        await page.click("body")
        await asyncio.sleep(8)  # Wait for 10 seconds to allow content to load
        # Extract outerHTML of the element
        main_body_html = await page.evaluate("""() => {
            const element = document.getElementById('react-application');
            return element ? element.outerHTML : null;
        }""")
        if main_body_html:
            if "data-is-day" in main_body_html:
                #with open(f"main_body_{check_in_str}_{id}.html", "w", encoding="utf-8") as f:
                #    f.write(main_body_html)            
                soup = BeautifulSoup(main_body_html, 'html.parser')
                listing_data = extract_listing_data(soup, id)
                all_data.append(listing_data)  
                print(f"Successfully scraped ID: {id}")
                
            else:
                print(f"calendar not found for id: {id}")
                #title_count = main_body_html.count('id="title"')
                import re
                title_count = len(re.findall(r'id="title', main_body_html))
                if title_count > 3:
                    unavailable_ids.append(id)
                    #with open(f"unavailable{check_in_str}_{id}.html", "w", encoding="utf-8") as f:
                    #    f.write(main_body_html) 
                else:
                    failed_ids.append(id)        
                                                                         
        else:
            print(f"Element not found for ID: {id}")
            failed_ids.append(id)
            
    except Exception as e:
        print(f"Error scraping ID {id}: {e}")
        failed_ids.append(id)
    finally:
        await browser.close()
            

async def scrape_all_ids():
    async with async_playwright() as playwright:
        batch_count = 1
        check_in_dates, check_out_dates = generate_check_dates()
        
        # Process IDs in chunks (batches)
        for batch in chunk_list(all_listing_ids_final, 1):  # Process 5 IDs at a time
            print(f"Processing Batch {batch_count}")
            
            # Create a task for each (ID, check_in, check_out) combination
            tasks = [
                scrape_id(playwright, id, check_in, check_out)
                for id in batch
                for check_in, check_out in zip(check_in_dates, check_out_dates)
            ]
            
            # Run all tasks concurrently
            await asyncio.gather(*tasks)
            
            print(f"Batch {batch_count} completed. Sleeping for 5 seconds...")
            global all_data
            ingest_data(all_data)
            all_data = []
            await asyncio.sleep(5)  # Sleep for 5 seconds between batches
            batch_count += 1


            if batch_count %25==0:
                # Insert data
                global unavailable_ids
                ingest_removed_id(unavailable_ids)
                unavailable_ids=[]              
                await asyncio.sleep(40)             
     

# Run the scraping
import time
start_time = time.time()
asyncio.run(scrape_all_ids())
end_time = time.time()
final_time=end_time-start_time
print(f"Total time taken for scraping: {final_time:.2f} seconds")
#print(f"unavailable_ids: {unavailable_ids}")





  