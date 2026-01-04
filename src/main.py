import argparse
import logging
import os
import sys
import yaml
import pytz
from datetime import datetime
from typing import Dict, List



from query_fabric_lakehouse import get_areas_list, get_specialised_carers_list, get_salesforce_data
from generate_powerbi_pdfs import (
    get_powerbi_access_token,
    generate_pdf_batch
)
from upload_to_sharepoint import (
    get_sharepoint_access_token,
    get_site_and_drive_id,
    upload_pdfs_batch,
    upload_text_content_to_sharepoint
)

from generate_sharepoint_links import(
    generate_employee_links,
    create_csv_content,
    upload_csv_to_sharepoint,
    create_link_generation_log,
    upload_log_to_sharepoint
)
from send_notification import send_bcp_notification


def setup_logging(config_path: str = None):
    log_file = 'fabric_pdf_generator.log'
    if config_path:
        try:
             with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                if 'logging' in config and 'file_path' in config['logging']:
                     log_file = config['logging']['file_path']
        except:
             pass

    logging.basicConfig(
        level = logging.INFO,
        format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file)
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

def load_config(config_path: str):
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        logger.info(f"Successfully loaded config from {config_path}")
        return config
    except Exception as e:
        logger.error(f"Failed to load config: {str(e)}")
        raise   

def get_environment_variables():
    required_vars = {
        #Fabric
        'FABRIC_TENANT_ID': os.getenv('FABRIC_TENANT_ID'),
        'FABRIC_CLIENT_ID': os.getenv('FABRIC_CLIENT_ID'),
        'FABRIC_CLIENT_SECRET': os.getenv('FABRIC_CLIENT_SECRET'),
        'FABRIC_SQL_ENDPOINT': os.getenv('FABRIC_SQL_ENDPOINT'),
        'FABRIC_DATABASE_NAME': os.getenv('FABRIC_DATABASE_NAME'),

        #Power BI
        'POWERBI_WORKSPACE_ID': os.getenv('POWERBI_WORKSPACE_ID'), 
        'POWERBI_REPORT_ID': os.getenv('POWERBI_REPORT_ID'),

        #SharePoint
        'SHAREPOINT_TENANT_ID': os.getenv('SHAREPOINT_TENANT_ID'),
        'SHAREPOINT_CLIENT_ID': os.getenv('SHAREPOINT_CLIENT_ID'),
        'SHAREPOINT_CLIENT_SECRET': os.getenv('SHAREPOINT_CLIENT_SECRET'),
        'SHAREPOINT_SITE_URL': os.getenv('SHAREPOINT_SITE_URL'),
        'SHAREPOINT_SITE_PATH': os.getenv('SHAREPOINT_SITE_PATH'),
        'SHAREPOINT_DRIVE_NAME': os.getenv('SHAREPOINT_DRIVE_NAME'),

        #Email
        'EMAIL_SENDER': os.getenv('EMAIL_SENDER'),
        'EMAIL_RECIPIENT': os.getenv('EMAIL_RECIPIENT'),
    }

    optional_vars = {
        'SMTP_SERVER': os.getenv('SMTP_SERVER'),
        'SMTP_PORT': os.getenv('SMTP_PORT'),
        'SMTP_PASSWORD': os.getenv('SMTP_PASSWORD'),
    }

    missing = [var for var in required_vars.values() if var is None]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    required_vars.update(optional_vars)
    logging.info("Successfully retrieved environment variables")

    return required_vars

def get_current_datetime(timezone_str: str = "Australia/Sydney"):
    tz = pytz.timezone(timezone_str)
    now = datetime.now(tz)
    date_str = now.strftime("%d-%m-%Y")
    datetime_str = now.strftime("%d-%m-%Y %H:%M:%S")
    timezone_str = now.strftime("%Y%m%d_%H%M%S")

    return date_str, datetime_str, timezone_str



def main():
    parser = argparse.ArgumentParser(description="Generate PDFs from PowerBI reports")
    parser.add_argument(
        '--config',
        type = str,
        default =  ' config/config.yaml',
        help= 'Path to config file'
    )
    parser.add_argument(
        '--report-type',
        type = str,
        choices = ['areas', 'employees', 'both'],
        default = 'both',
        help = 'Type of report to generate'
    )
    parser.add_argument(
        '--dry-run',
        action = 'store_true',
        help = 'Run the script in dry run mode'
    )
    parser.add_argument(
        '--skip-links',
        action = 'store_true',
        help = 'Skip link generation'
    )
    parser.add_argument(
        '--batch-size',
        type = int,
        default = None,
        help = 'Batch size for PDF generation'
    )

    args = parser.parse_args()

    
    logger.info("="*60)
    logger.info("PowerBI PDF Generator workflow started")
    logger.info("="*60)

    if args.dry_run:
        logger.info("Dry run mode enabled - no uploads or emails will be sent")

    try:
        config = load_config(args.config)
        env_vars = get_environment_variables()
        
        date_str, datetime_str, timezone_str = get_current_datetime(config.get('timezone', 'Australia/Sydney'))
        logger.info(f"Current date and time: {datetime_str}")

        area_list = []
        area_success_count = 0
        area_failure_count = 0

        employee_list = []
        employee_success_count = 0
        employee_failure_count = 0

        logger.info("\n" + "="*60)
        logger.info("STEP 1 : Querying Fabric Lakehouse")
        logger.info("="*60)

        if args.report_type in ['areas', 'both']:
            logger.info("Fetching area list...")
            area_list = get_areas_list(
                env_vars['FABRIC_TENANT_ID'],
                env_vars['FABRIC_CLIENT_ID'],
                env_vars['FABRIC_CLIENT_SECRET'],
                env_vars['FABRIC_SQL_ENDPOINT'],
                env_vars['FABRIC_DATABASE_NAME'],
                config['queries']['areas']
            )
            logger.info(f"Fetched {len(area_list)} areas")

        if args.report_type in ['employees', 'both']:
            logger.info("Fetching employee list...")
            employee_list = get_specialised_carers_list(                #TODO: change function name to more generic get_employee_list in query_fabric_lakehouse.py
                env_vars['FABRIC_TENANT_ID'],
                env_vars['FABRIC_CLIENT_ID'],
                env_vars['FABRIC_CLIENT_SECRET'],
                env_vars['FABRIC_SQL_ENDPOINT'],
                env_vars['FABRIC_DATABASE_NAME'],
                config['queries']['employees']
            )
            logger.info(f"Fetched {len(employee_list)} employees")


        logger.info("\n" + "="*60)
        logger.info("STEP 2 : Authenticating with PowerBI")
        logger.info("="*60)

        powerbi_access_token = get_powerbi_access_token(
            env_vars['FABRIC_TENANT_ID'],
            env_vars['FABRIC_CLIENT_ID'],
            env_vars['FABRIC_CLIENT_SECRET']
        )

        logger.info("\n" + "="*60)
        logger.info("STEP 3 : Authenticating with SharePoint")
        logger.info("="*60)

        sharepoint_access_token = get_sharepoint_access_token(
            env_vars['SHAREPOINT_TENANT_ID'],
            env_vars['SHAREPOINT_CLIENT_ID'],
            env_vars['SHAREPOINT_CLIENT_SECRET']
        )

        site_id, drive_id = get_site_and_drive_id(
            sharepoint_access_token,
            env_vars['SHAREPOINT_SITE_URL'],
            env_vars['SHAREPOINT_SITE_PATH'],
            env_vars['SHAREPOINT_DRIVE_NAME']
        )

        if args.report_type in ['areas', 'both'] and area_list:
            logger.info("\n" + "="*60)
            logger.info("STEP 4 : Generating and uploading PDFs for areas")
            logger.info("="*60)

            batch_size_areas =  args.batch_size if args.batch_size else config['processing']['batch_size_areas']

            areas_pdfs = generate_pdf_batch(
                area_list,
                env_vars['POWERBI_WORKSPACE_ID'],
                env_vars['POWERBI_REPORT_ID'],
                powerbi_access_token,
                batch_size_areas,
                config['processing']['max_retries'] if 'max_retries' in config['processing'] else 3,
                config['powerbi']['export']['retry_interval_seconds'],
                config['processing']['retry_delay_seconds']
            )

            areas_success_count = len(areas_pdfs)
            areas_failed_count = len(area_list) - areas_success_count

            if not args.dry_run and areas_pdfs:
                logger.info(f"Uploading {len(areas_pdfs)} area PDFs to SharePoint")
                upload_pdfs_batch(
                    areas_pdfs,
                    sharepoint_access_token,
                    site_id,
                    drive_id,
                    config['sharepoint']['folders']['areas']
                )
                logger.info(f"Successfully uploaded {len(areas_pdfs)} area PDFs to SharePoint, Failed: {areas_failed_count}")

            log_content = f"Areas PDFs generated log\ndate: {datetime_str}\n\n"
            log_content += f"Total: {len(area_list)}\n"
            log_content += f"Success: {areas_success_count}\n"
            log_content += f"Failed: {areas_failed_count}\n"

            if areas_failed_count > 0:
                failed_areas = [area for area in area_list if area not in areas_pdfs]
                log_content += "Failed Areas:\n" + "\n".join(f" - {area}" for area in failed_areas)

            if not args.dry_run:
                log_filename = f"Logs_Areas_{datetime_str}.txt"
                upload_text_content_to_sharepoint(
                    sharepoint_access_token,
                    drive_id,
                    config['sharepoint']['folders']['logs'],
                    log_filename,
                    log_content
                )

            if args.report_type in ['employees', 'both'] and employee_list:
                logger.info("\n" + "="*60)
                logger.info("STEP 5 : Generating and uploading PDFs for employees")
                logger.info("="*60)

                employee_names = [emp['Name'] for emp in employee_list]

                batch_size_employees = args.batch_size if args.batch_size else config['processing']['batch_size_employees']

                employees_pdfs = generate_pdf_batch(
                    employee_names,
                    env_vars['POWERBI_WORKSPACE_ID'],
                    env_vars['POWERBI_REPORT_ID'],
                    powerbi_access_token,
                    batch_size_employees,
                    config['processing']['max_retries'] if 'max_retries' in config['processing'] else 3,
                    config['powerbi']['export']['retry_interval_seconds'],
                    config['processing']['retry_delay_seconds']
                )   

                employees_success_count = len(employees_pdfs)
                employees_failed_count = len(employee_list) - employees_success_count

                if not args.dry_run and employees_pdfs:
                    logger.info(f"Uploading {len(employees_pdfs)} employee PDFs to SharePoint")
                    upload_pdfs_to_sharepoint(
                        employees_pdfs,
                        sharepoint_access_token,
                        drive_id,
                        config['sharepoint']['folders']['employees'],
                        50,
                        create_folders=True
                    )
                    logger.info(f"Successfully uploaded {len(employees_pdfs)} employee PDFs to SharePoint, Failed: {employees_failed_count}")

                log_content = f"Employees PDFs generated log\ndate: {datetime_str}\n\n"
                log_content += f"Total: {len(employee_list)}\n"
                log_content += f"Success: {employees_success_count}\n"
                log_content += f"Failed: {employees_failed_count}\n"

                if employees_failed_count > 0:
                    failed_employees = [emp for emp in employee_list if emp not in employees_pdfs]
                    log_content += "Failed Employees:\n" + "\n".join(f" - {emp}" for emp in failed_employees)

                if not args.dry_run:
                    log_filename = f"Logs_Employees_{datetime_str}.txt"
                    upload_text_content_to_sharepoint(
                        log_content,
                        sharepoint_access_token,
                        drive_id,
                        config['sharepoint']['folders']['logs'],
                        log_filename
                    )


            if not args.skip_links and args.report_type in ['employees', 'both'] and employee_list and not args.dry_run:
                
                logger.info("\n" + "="*60)
                logger.info("STEP 6 : Generating shareable links for employee PDFs")
                logger.info("="*60) 

                success_records, failed_records = generate_employee_links(
                    employee_list,
                    sharepoint_access_token,
                    drive_id,
                    config['sharepoint']['folders']['employees'],
                    config['processing'].get('link_generation_batch_size', 50)
                )

                all_records = success_records + failed_records
                csv_content = create_csv_content(all_records)

                csv_filename = "Shareable_Links_Employees.csv"

                upload_text_content_to_sharepoint(
                    csv_content,
                    sharepoint_access_token,
                    drive_id,
                    csv_filename,
                    "" #root folder
                )

                log_content = create_link_generation_log(
                    len(employee_list),
                    len(success_records),
                    len(failed_records),
                    datetime_str,
                    failed_records
                )

                log_filename = config['link_generation']['log_filename_template'].format(timestamp=datetime_str)
                upload_text_content_to_sharepoint(
                    log_content,
                    sharepoint_access_token,
                    drive_id,
                    log_filename,
                    config['sharepoint']['folders']['logs']
                )


            if not args.dry_run:
                logger.info("\n" + "="*60)
                logger.info("STEP 7 : Sending email notifications")
                logger.info("="*60) 

                recipeints = env_vars['EMAIL_RECIPIENTS'].split(",")

                smtp_port = None 
                if env_vars['SMTP_PORT'] != 'None':
                    try: 
                        smtp_port = int(env_vars['SMTP_PORT'])
                    except ValueError:
                        logger.error(f"Invalid SMTP port: {env_vars['SMTP_PORT']}")

                send_bcp_notification(
                    env_vars['SHAREPOINT_TENANT_ID'],
                    env_vars['SHAREPOINT_CLIENT_ID'],
                    env_vars['SHAREPOINT_CLIENT_SECRET'],
                    env_vars['EMAIL_SENDER'],
                    recipeints,
                    date_str,
                    len(area_list),
                    area_success_count,
                    areas_failed_count,
                    len(employee_list),
                    employees_success_count,
                    employees_failed_count,
                    smtp_server = env_vars['SMTP_SERVER'],
                    smtp_port = smtp_port,
                    smtp_password = env_vars['SMTP_PASSWORD'],
                    use_smtp_fallback = True
                ) 

            logger.info("\n" + "="*60)
            logger.info("STEP 8 : Completed")
            logger.info("="*60)
            logger.info(f"Summary") 
            logger.info(f"Areas: {area_success_count} successful, {areas_failed_count} failed")
            logger.info(f"Employees: {employees_success_count} successful, {employees_failed_count} failed")

            return 0
    except Exception as e:
        logger.error(f"Workflow failed with error: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())


                