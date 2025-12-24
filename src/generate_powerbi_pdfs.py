import time
import logging
import requests
from io import BytesIO
from typing import Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.identity import ClientSecretCredential

logger = logging.getLogger(__name__)

def get_powerbi_access_token(tenant_id: str, client_id: str, client_secret: str):
    try:
        logger.info("Authenticating to Power BI using service principal")
        credential = ClientSecretCredential(
            tenant_id = tenant_id,
            client_id = client_id,
            client_secret = client_secret
        )
        token = credential.get_token("https://analysis.windows.net/powerbi/api")
        logger.info("Successfully authenticated to Power BI")
        return token.token
    except Exception as e:
        logger.error(f"Failed to authenticate to Power BI: {str(e)}")
        raise


def export_report_to_pdf(workspace_id: str, report_id: str, parameter_name: str, access_token: str, max_retries: int = 30, retry_interval: int = 10):
    try:
        base_url = "https://api.powerbi.com/v1.0/myorg" 
        export_url = f"{base_url}/groups/{workspace_id}/reports/{report_id}/ExportTo"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        export_body = {
            "format":"PDF",
            "paginatedReportConfiguration":{
                "parameterValues":[
                    {"name":parameter_name, "value":parameter_value}
                ]
            }
        }

        logger.info(f"Initiating PDF export for parameter '{parameter_name}' = '{parameter_value}'")
        export_response = requests.post(export_url, headers=headers, json=export_body)

        if export_response.status_code != 200:
            logger.error(f"Failed to initiate PDF export: {export_response.status_code} - {export_response.text}")
            return None
        
        export_id = export_response.json().get("id")
        if not export_id:
            logger.error(f"Failed to retrieve export ID from response")
            return None

        logger.info(f"Export initiated successfully with ID: {export_id}")
        export_status_url = f"{base_url}/groups/{workspace_id}/reports/{report_id}/exports/{export_id}"

        retry_count = 0

        while retry_count<max_retries:
            status_response = requests.get(export_status_url, headers=headers)

            if status_response.status_code not in [200,202]:
                logger.error(f"Failed to get export status: {status_response.status_code}")
                return None
            status_data = status_response.json()
            status = status_data.get("status","Running")

            if status ==  "Succeeded":
                logger.info(f"Export comploted successfully for {parameter_value}")

                result_url = f"{export_status_url}/file"
                file_response = requests.get(result_url, headers=headers)

                if file_response.status_code == 200:

                    pdf_content = BytesIO(file_response.content)
                    logger.info(f"PDF download to memory ({len(file_response.content)} bytes)")
                    return pdf_content
                else:
                    logger.error(f"Failed to download PDF: {file_response.status_code}")
                    return None
            elif status in ['Running', 'NotStarted']:
                logger.info(f"Export in progress... Status: {status} (attempt {retry_count + 1}/{max_retries})")
                time.sleep(retry_interval)
                retry_count +=1
            else:
                logger.error(f"Export failed with status: {status}")
                return None
        logger.error(f"Export failed after {max_retries} attempts")
        return None
    except Exception as e:
        logger.error(f"Failed to export report to PDF: {str(e)}")
        return None

def generate_pdf_batch(values: list, workspace_id: str, report_id: str, parameter_name: str, access_token: str, batch_size: int = 20, max_retries: int = 3, retry_interval: int = 10, retry_delay: int = 5):
    pdf_dict = {}
    failed_items = []
    total_items = len(values)

    logger.info(f"Starting batch processing for {total_items} items with batch size {batch_size}")

    with ThreadPoolExecutor(max_workers=batch_size) as executor:
        futures = {}

        for value in values:
            future = executor.submit(
                export_report_to_pdf,
                workspace_id,
                report_id,
                parameter_name,
                value,
                access_token,
                30,
                retry_interval
            )
            futures[future] = value

        for future in as_completed(futures):
            value = futures[future]
            try:
                pdf_content = future.result()
                if pdf_content:
                    pdf_dict[value] = pdf_content
                    logger.info(f"Successfully generated PDF for {value}")
                else:
                    logger.warning(f"⛔ Failed to generate PDF for: {value} (will retry)")
                    failed_items.append(value)
            except Exception as e:
                logger.error(f"Failed to generate PDF for {value}: {str(e)}")
                failed_items.append(value)
    

    retry_attempt = 1
    while failed_items and retry_attempt <= max_retries:
        logger.info(f"")
        logger.info(f"Retrying failed items (attempt {retry_attempt}/{max_retries}) for {len(failed_items)} items")
        logger.info(f"Failed items: {','.join(failed_items)}")

        logger.info(f"Waiting for {retry_delay} seconds before retrying...")
        time.sleep(retry_delay)


        retry_failed = []

        retry_batch_size = min(1, batch_size // 2)

        with ThreadPoolExecutor(max_retries=retry_batch_size) as executor:
            retry_futures = {}

            for value in failed_items:
                future = executor.submit(
                    export_report_to_pdf,
                    workspace_id,
                    report_id,
                    parameter_name,
                    value,
                    access_token,
                    30,
                    retry_interval
                )


                retry_futures[future] = value

                for future in as_completed(retry_futures):
                    value = retry_futures[future]
                    try:
                        pdf_content = future.result()
                        if pdf_content:
                            pdf_dict[value] = pdf_content
                            logger.info(f"Successfully generated PDF for {value}")
                        else:
                            logger.warning(f"⛔ Failed to generate PDF for: {value} (will retry)")
                            retry_failed.append(value)
                    except Exception as e:
                        logger.error(f"Failed to generate PDF for {value}: {str(e)}")
                        retry_failed.append(value)

        failed_items = retry_failed
        retry_attempt += 1  

    success_count = len(pdf)
    failed_count = len(failed_items)

    logger.info("")
    logger.info("=" * 60)
    logger.info(f"PDF Generation Summary: ")
    logger.info(f"Total items: {total_items}")
    logger.info(f"Successfully generated PDFs: {success_count}")
    logger.info(f"Failed to generate PDFs: {failed_count}")
    if failed_items:
        logger.info("Failed items: ", ",".join(failed_items))
    logger.info("=" * 60)
