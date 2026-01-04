import logging
import requests
import time
from typing import List, Dict, Tuple
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed 


logger = logging.getLogger(__name__)

def generate_single_employee_link(employee: Dict, access_token: str, drive_id: str, base_folder: str, headers: Dict, retry_count: int = 0, max_retries: int = 3):
    try:
        # Try to find ID and Name fields dynamically if standard keys aren't present
        employee_id = employee.get('EmployeeID') or employee.get('ID') or employee.get('id')
        if not employee_id and employee:
            # Fallback: use the first value found if it looks like an ID (digit)
             for k, v in employee.items():
                 if str(v).isdigit():
                     employee_id = v
                     break
        
        employee_id = str(employee_id) if employee_id else "UnknownID"
        
        # Remove hardcoded padding logic. If padding is needed, it should be handled in the SQL query or config.
        
        employee_name = employee.get('EmployeeName') or employee.get('Name') or employee.get('name') or "UnknownName"
        employee_email = employee.get('EmployeeEmail') or employee.get('Email') or employee.get('email') or ""
        
        # Use name_identifier for file naming
        name_identifier = employee.get('Name') or employee_name

        folder_name = f"report_{name_identifier}"
        pdf_filename = f"report_{name_identifier}.pdf"
        file_path = f"{base_folder}/{folder_name}/{pdf_filename}" 

        file_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{file_path}"
        file_response = requests.get(file_url, headers=headers)

        if file_response.status_code == 429:
            if retry_count < max_retries:
                wait_time = 2 ** retry_count
                logger.warning(f"Throttled checking file for {employee_name}, waiting {wait_time}s (retry {retry_count +1}/{max_retries})")
                time.sleep(wait_time)
                return generate_single_employee_link(employee, access_token, drive_id, base_folder, headers, retry_count + 1, max_retries)
            else:
                logger.error(f"Max retries exceeded for {employee_name} due to throttling")
                return {
                    'employee_id': employee_id,
                    'employee_name': employee_name,
                    'employee_email': employee_email,
                    'sharepoint_link': 'Throttled',
                    'status': 'Failed'
                }, False

        if file_response.status_code !=200:
            logger.warning(f"File not found for {employee_name} (ID: {employee_id}) at {file_path}")
            return {
                'employee_id': employee_id,
                'employee_name': employee_name,
                'employee_email': employee_email,
                'sharepoint_link': 'File Not Found',
                'status': 'Failed'
            }, False

        file_item = file_response.json()
        file_id = file_item['id']

        link_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{file_id}/createLink"
        link_payload = {
            "type": "view",
            "scope": "organization"
        }

        link_response = requests.post(link_url, headers=headers, json=link_payload)

        if link_response.status_code == 429:

            if retry_count < max_retries:
                wait_time = 2 ** retry_count
                logger.warning(f"Throttled creating link for {employee_name}, waiting {wait_time}s (retry {retry_count +1}/{max_retries})")
                time.sleep(wait_time)
                return generate_single_employee_link(employee, access_token, drive_id, base_folder, headers, retry_count + 1, max_retries)
            else:
                logger.error(f"Max retries exceeded for {employee_name} due to throttling")
                return {
                    'employee_id': employee_id,
                    'employee_name': employee_name,
                    'employee_email': employee_email,
                    'sharepoint_link': 'Throttled',
                    'status': 'Failed'
                }, False    
        
        if link_response.status_code in [200, 201]:
            sharepoint_link = link_response.json()['link']['webUrl']
            logger.info(f"✅ Successfully generated sharepoint link for {employee_name} (ID: {employee_id})")
            return {
                'employee_id': employee_id,
                'employee_name': employee_name,
                'employee_email': employee_email,
                'sharepoint_link': sharepoint_link,
                'status': 'Success'
            }, True
        else:
            logger.error(f"Failed to generate sharepoint link for {employee_name} (ID: {employee_id})")
            return {
                'employee_id': employee_id,
                'employee_name': employee_name,
                'employee_email': employee_email,
                'sharepoint_link': 'Failed',
                'status': 'Failed'
            }, False
    except Exception as e:
        logger.error(f"Exception processing employee {employee.get('Name', 'Unknown')}: {str(e)}")
        return {
            'employee_id': employee_id,
            'employee_name': employee_name,
            'employee_email': employee_email,
            'sharepoint_link': 'Failed',
            'status': 'Failed'
        }, False


def generate_employee_links(employee_list: List[Dict], access_token: str, drive_id: str, base_folder: str = "Employees", batch_size: int = 50):
    success_records = []
    failed_records = []
    
    safe_batch_size = min(batch_size, 15)

    logger.info(f"Generating shareable links for {len(employee_list)} employees (batch size: {safe_batch_size})")
    logger.info("Using reduced batch size to avoid Sharepoint API throttling")

    headers = {"Authorization": f"Bearer {access_token}"}

    with ThreadPoolExecutor(max_workers=safe_batch_size) as executor:
        futures = {}

        for employee in employee_list:
            future = executor.submit(
                generate_single_employee_link,
                employee,
                access_token,
                drive_id,
                base_folder,
                headers
            )
            futures[future] = employee.get('Name', 'Unknown')

        completed = 0

        for future in as_completed(futures):
            employee_name = futures[future]
            try:
                record, success = future.result()
                if success:
                    success_records.append(record)
                else:
                    failed_records.append(record)
                if completed % 50 == 0:
                    logger.info(f"Progress: {completed}/{len(employee_list)} links processed")
            
            except Exception as e:
                logger.error(f"Exception processing employee {employee_name}: {str(e)}")
                failed_records.append({
                    'employee_id'   : employee.get('ID', 'Unknown'),
                    'employee_name': employee_name,
                    'employee_email': employee.get('Email', 'Unknown'),
                    'sharepoint_link': 'Error',
                    'status': 'Error'
                })

    logger.info(f"Link generation completed for {len(employee_list)} employees, Failed: {len(failed_records)}")
    return success_records, failed_records


def create_csv_content(records: List[Dict]):
    try:
        import pandas as pd 

        df = pd.DataFrame(records)
        csv_content = df.to_csv(index=False)

        logger.info(f"Created csv with {len(records)} records")
        return csv_content
    except Exception as e:
        logger.error(f"Exception creating CSV content: {str(e)}")
        return None 


def upload_csv_to_sharepoint(csv_content: str, access_token: str, drive_id: str, file_name: str, folder_path: str = "", max_retries: int = 5):
    headers = {"Authorization": f"Bearer {access_token}"}

    if folder_path:
        upload_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{folder_path}/{file_name}:content"
    else:
        upload_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{file_name}:content"
    
    logger.info(f"Uploading CSV to Sharepoint: '{upload_url}' to sharepoint drive '{drive_id}'")

    for attempt in range(max_retries):
        try:
            response = requests.put(upload_url, headers=headers, data=csv_content.encode('utf-8'))

            if response.status_code in [200, 201]:
                logger.info(f"✅ Successfully uploaded CSV to Sharepoint: {file_name}")
                return True
            elif response.status_code == 424:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"Throttled uploading CSV, waiting {wait_time}s (retry {attempt +1}/ {max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Max retries exceeded for uploading CSV to Sharepoint due to Throttling")
                    return False
            else:
                logger.error(f"Failed to upload CSV to Sharepoint: {response.status_code} - {response.text}")
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.info (f"Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    return False

        except Exception as e:
            logger.error(f"Exception uploading CSV to Sharepoint: {str(e)}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                logger.info(f"Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                return False   
        
    return False


def create_link_generation_log(total_count: int, success_count: int, failed_count: int, process_datetime: str, failed_records: List[Dict]):

    log_content = f"""
------------------------
Link Generation Log
------------------------
Total Count: {total_count}
Success Count: {success_count}
Failed Count: {failed_count}
Process DateTime: {process_datetime}
------------------------
""" 

    if failed_records:
        log_content += "Failed Records:\n"
        for record in failed_records:
            log_content += f" - {record.get('Name', 'Unkmown')} (ID:{record.get('ID', 'Unknown')}): {record.get('sharepoint_link', 'Unknown')}\n"
    return log_content


def upload_log_to_sharepoint(log_content: str, access_token: str, drive_id: str, file_name: str, folder_path: str = "Logs", max_retries: int = 5):

    headers = {"Authorization": f"Bearer {access_token}"}
    upload_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{folder_path}/{file_name}:content"

    logger.info(f"Uploading log file '{file_name}' to sharepoint drive '{drive_id}'")

    for attempt in range( max_retries):
        try:
            response = requests.put(upload_url, headers=headers, data=log_content.encode('utf-8'))
            if  response.status_code in [200, 201]:
                logger.info(f"✅ Successfully uploaded log file '{file_name}' to sharepoint drive '{drive_id}'")
                return True
            elif response.status_code == 424:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"Throttled uploading log file, waiting {wait_time}s (retry {attempt +1}/ {max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Max retries exceeded for uploading log file to sharepoint due to Throttling")
                    return False
            else:
                logger.error(f"Failed to upload log file to sharepoint: {response.status_code} - {response.text}")
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.info (f"Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    return False

        except Exception as e:
            logger.error(f"Exception uploading log file to sharepoint: {str(e)}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                logger.info(f"Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                return False   
        
    return False    