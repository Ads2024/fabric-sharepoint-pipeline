import logging
import requests
import msal 
from io import BytesIO
from typing import Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)


def get_sharepoint_access_token(tenant_id: str, client_id: str, client_secret: str):
    try:
        
        logger.info("Authenticating with Microsoft Graph API for SharePoint")

        authority = f"https://login.microsoftonline.com/{tenant_id}"
        scope = ["https://graph.microsoft.com/.default"]

        app = msal.ConfidentialClientApplication(
            client_id,
            authority=authority,
            client_credential=client_secret
        )

        result = app.acquire_token_for_client(scopes=scope)
        
        if "access_token" not in result:
            raise Exception(f"Failed to acquire token: {result.get('error_description', 'Unknown error')}")
        
        logger.info("Successfully obtained Microsoft Graph access token for SharePoint")
        return result["access_token"]
    
    except Exception as e:
        logger.error(f"Failed to obtain Microsoft Graph access token for SharePoint: {e}")
        raise   



def get_site_and_drive_id(access_token: str, site_url: str, site_path: str, drive_name: str):
    try:
        header = {"Authorization": f"Bearer {access_token}"}
        domain = site_url.replace("https://", "").replace("http://", "").split("/")[0]

        site_path_clean = site_path.strip().strip("/")
        if not site_path_clean.startswith(f"/"):
            site_path_clean = '/' + site_path_clean

        logger.info(f"Getting site ID for {domain}:{site_path_clean}")

        graph_site_url = f"https://graph.microsoft.com/v1.0/sites/{domain}:/sites/{site_path_clean}"

        logger.debug(f"Site URL: {graph_site_url}")

        site_response = requests.get(graph_site_url, headers=header)

        if site_response.status_code != 200:
            raise Exception(f"Failed to get site ID: {site_response.status_code} - {site_response.text}")

        site_data = site_response.json()
        site_id = site_data['id']
        logger.info(f"Successfully obtained site ID: {site_id}")

        logger.info(f"Getting drive ID for drive: {drive_name}")
        drive_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"

        drives_response = requests.get(drive_url, headers = header)

        if drives_response.status_code != 200:
            raise Exception(f"Failed to get drive ID: {drives_response.status_code} - {drives_response.text}")

        drives_data = drives_response.json()

        drive_id = None
        for drive in drives_data.get("value", []):
            if drive.get("name") == drive_name:
                drive_id = drive["id"]
                break

        if not drive_id:
            available_drives = [d.get("name") for d in drives_data.get("value", [])]
            raise Exception(f"Drive {drive_name} not found. Available drives: {available_drives}")
        
        logger.info(f"Successfully obtained drive ID: {drive_id}")
        return site_id, drive_id
    
    except Exception as e:
        logger.error(f"Failed to get site and drive ID: {e}")
        raise   



def ensure_folder_exists(access_token: str, drive_id: str, folder_path: str ):
    try:
        header = {"Authorization": f"Bearer {access_token}"}
        
        folder_path_clean = folder_path.strip().strip("/")
        if not folder_path_clean:
            return True

        check_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{folder_path_clean}"
        response = requests.get(check_url, headers = header)

        if response.status_code == 200:
            logger.debug(f"Folder already exists: {folder_path_clean}")
            return True

        logger.info(f"Creating folder: {folder_path_clean}")

        parts = folder_path_clean.split("/")
        current_path = ""

        for part in parts:
            if current_path:
                current_path += f"/{part}"

            else:
                current_path = part

            check_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{current_path}"
            response = requests.get(check_url, headers = header)

            if response.status_code == 200:
                parent_path = "/".join(current_path.split("/")[:-1])

                if parent_path:
                    create_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{parent_path}:/children"
                else:
                    create_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/children"

                payload = {
                    "name": part,
                    "folder": {},
                    "@microsoft.graph.conflictBehavior": "rename"
                }

                create_response = requests.post(create_url, headers = header, json = payload)

                if create_response.status_code not in [200, 201]:
                    logger.error(f"Failed to create folder '{part}' : {create_response.status_code} - {create_response.text}")
                    return False
                logger.debug(f"Created Folder level: {current_path}")

        logger.info(f"Successfully created folder: {folder_path_clean}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to create folder: {folder_path_clean}")
        return False    


def upload_pdf_stream_to_sharepoint(access_token: str, drive_id: str, folder_path: str, file_name: str, pdf_content: BytesIO, create_folder: bool = True):
    try:
        if create_folder and folder_path:
            if not ensure_folder_exists(access_token, drive_id, folder_path):
                return False

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/pdf"
        }

        folder_path_clean = folder_path.strip().strip("/")

        if not file_name.lower().endswith(".pdf"):
            file_name += ".pdf" 

        if folder_path_clean:
            upload_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{folder_path_clean}:/{file_name}:/content"

        else:
            upload_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{file_name}:/content"

        pdf_content.seek(0)
        response = requests.put(upload_url, headers = headers, data = pdf_content.read())

        if response.status_code in [200,201]:
            logger.info(f"Successfully uploaded PDF: {file_name}")
            return True
        else:
            logger.error(f"Failed to upload PDF: {file_name} : {response.status_code} - {response.text}")
            return False
    
    except Exception as e:
        logger.error(f"Failed to upload PDF: {file_name} : {e}")
        return False    


def upload_text_content_to_sharepoint(access_token: str, drive_id: str, folder_path: str, file_name: str, content: str, create_folder: bool = True):

    try:
        if create_folder and folder_path:
            if not ensure_folder_exists(access_token, drive_id, folder_path):
                logger.error(f"Cannot upload {file_name} - failed to create folder: {folder_path}")
                return False

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "text/plain"
        }

        folder_path_clean = folder_path.strip().strip("/")

        if not file_name.lower().endswith(".txt"):
            file_name += ".txt" 

        if folder_path_clean:
            upload_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{folder_path_clean}:/{file_name}:/content"

        else:
            upload_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{file_name}:/content"

        response = requests.put(upload_url, headers = headers, data = content.encode("utf-8"))

        if response.status_code in [200,201]:
            logger.info(f"Successfully uploaded text file: {file_name}")
            return True
        else:
            logger.error(f"Failed to upload text file: {file_name} : {response.status_code} - {response.text}")
            return False
    
    except Exception as e:
        logger.error(f"Failed to upload text file: {file_name} : {e}")
        return False  


def upload_pdfs_batch(pdf_dict: dict, access_token: str, drive_id: str, base_folder: str, batch_size: int = 50, create_folder: bool = False):
    successful = []
    failed = []

    logger.info(f"Stating batch upload of {len(pdf_dict)} files to {base_folder}")    
    if create_folder:
        logger.info(f"Creating individual folders for each file (folder structure: base/report_name_id/report_name_id.pdf)")

    with ThreadPoolExecutor(max_workers=batch_size) as executor:
        futures = {}

        for identifier, pdf_content in pdf_dict.items():
            if not identifier.endswith(".pdf"):
                file_name = f"{identifier}.pdf"
            else:
                file_name = identifier

            if create_folder:
                clean_identifier = identifier.replace('report_', '').replace('.pdf','')
                folder_name = f"report_{clean_identifier}"
                folder_path = f"{base_folder}/{folder_name}"
                final_file_name = f"report_{clean_identifier}.pdf"

            else:
                folder_path = base_folder
                final_file_name = file_name 

            future = executor.submit(
                upload_pdf_stream_to_sharepoint,
                access_token,
                drive_id,
                folder_path,
                final_file_name,
                pdf_content,
                create_folder
            )
            futures[future] = identifier
        for future in as_completed(futures):
            identifier = futures[future]
            try:
                if future.result():
                    successful.append(identifier)
                else:
                    failed.append(identifier)
            except Exception as e:
                logger.error(f"Failed to upload PDF: {identifier} : {e}")
                failed.append(identifier)
    
    logger.info(f"Successfully uploaded {len(successful)} files")
    logger.info(f"Failed to upload {len(failed)} files")
    return successful, failed   




def generate_shareable_link(access_token: str, drive_id: str, file_path: str):
    try:
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        file_path_clean = file_path.strip().strip("/")

        create_link_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{file_path_clean}:/createLink"

        payload = {
            "type": "view",
            "scope": "organization"
        }


        response = requests.post(create_link_url, headers = headers, json = payload)

        if response.status_code in [200,201]:
            link_data = response.json()
            return link_data.get("link").get("webUrl")
        else:
            logger.error(f"Failed to create shareable link: {file_path} : {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logger.error(f"Failed to create shareable link: {file_path} : {e}")
        return None
