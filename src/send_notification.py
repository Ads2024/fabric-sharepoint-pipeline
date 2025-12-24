from typing import Optional
import logging
import requests
import smtplib
import msal
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Dict

logger = logging.getLogger(__name__)

def get_graph_access_token(tenant_id: str, client_id: str, client_secret: str):
    try:

        logger.info("Authenticating with Microsoft Graph API for email")

        authority = f"https://login.microsoftonline.com/{tenant_id}"
        app = msal.ConfidentialClientApplication(client_id, authority=authority, client_credential=client_secret)
        result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])

        if 'access_token' not in result:
            raise Exception(f"Failed to aquire token: {result.get('error_description', 'Unknown error')}")

        logger.info("Successfully obtained Microsoft Graph access token for email")
        return result['access_token']
    
    except Exception as e:
        logger.error(f"Failed to obtain Microsoft Graph access token for email: {e}")
        raise   


def create_email_html_body(date_str: str, areas_total: int, areas_success: int, areas_failed: int, employees_total: int, employees_success: int, employees_failed: int):
    html_body = f"""
    <html>
    <head>
        <style>
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                line-height: 1.6;
                color: #333;
            }}
            .container {{
                max-width: 600px;
                margin: 0 auto;
                padding: 20px;
            }}
            h2 {{
                color: #0078d4;
                border-bottom: 2px solid #0078d4;
                padding-bottom: 10px;
            }}
            .section {{
                background-color: #f5f5f5;
                border-left: 4px solid #0078d4;
                padding: 15px;
                margin: 15px 0;
            }}
            .success {{
                color: #107c10;
                font-weight: bold;
            }}
            .failure {{
                color: #d13438;
                font-weight: bold;
            }}
            .stats {{
                font-size: 16px;
                line-height: 2;
            }}
            .footer {{
                margin-top: 30px;
                padding-top: 20px;
                border-top: 1px solid #ccc;
                font-size: 12px;
                color: #666;
            }}
        </style>
    </head>

    <body>
        <div class="container">
            <h2>Salesforce BCP PDF Generation Report</h2>

            <p>Hello,</p>
            <p>Today is <strong>{date_str}</strong>.</p>

            <div class="section">
                <h3>📊 Areas</h3>
                <div class="stats">
    """

    if areas_failed == 0:
        html_body += f"""
                    <span class="success">✅ {areas_success} records</span> have been created successfully.<br>
                    Total count of areas: <strong>{areas_total}</strong>
        """
    else:
        html_body += f"""
                    <span class="success">✅ {areas_success} records</span> have been created successfully.<br>
                    <span class="failure">❌ {areas_failed} failed</span> to be created.<br>
                    Total count of areas: <strong>{areas_total}</strong>
        """

    html_body += """
                </div>
            </div>

            <div class="section">
                <h3>👥 Specialised Carers</h3>
                <div class="stats">
    """

    if employees_failed == 0:
        html_body += f"""
                    <span class="success">✅ {employees_success} Specialised Carers records</span> have been created successfully.<br>
                    Total count of Specialised Carers: <strong>{employees_total}</strong>
        """
    else:
        html_body += f"""
                    <span class="success">✅ {employees_success} Specialised Carers records</span> have been created successfully.<br>
                    <span class="failure">❌ {employees_failed} failed</span> to be created.<br>
                    Total count of Specialised Carers: <strong>{employees_total}</strong>
        """

    html_body += """
                </div>
            </div>

            <div class="footer">
                <p>This is an automated notification from the BCP PDF Generation workflow.</p>
                <p>For any issues, please check the logs in SharePoint.</p>
            </div>
        </div>
    </body>
    </html>
    """
    return html_body

def send_email_via_smtp(smtp_server: str, smtp_port: int, sender_email: str, sender_password: str, receipient_emails: List[str], subject: str, html_body: str, cc_emails: Optional[List[str]] = None, use_tls: bool = True):
    try:
        logger.info(f"Sending email via SMTP to {len(receipient_emails)} recipients")

        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = sender_email
        msg['To'] = ', '.join(receipient_emails)
        
        if cc_emails:
            msg['Cc'] = ', '.join(cc_emails)
        
        html_part = MIMEText(html_body, 'html')
        msg.attach(html_part)

        with smtplib.SMTP(smtp_server, smtp_port) as server:
            if use_tls:
                server.starttls()
            
            server.login(sender_email, sender_password)

            all_recipients = receipient_emails + (cc_emails or [])
            server.send_message(msg, from_addr=sender_email, to_addrs=all_recipients)
            
            logger.info("Email sent successfully")
            return True
        
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False 



def send_email_via_graph(access_token: str, sender_email: str, receipient_emails: List[str], subject: str, html_body: str, cc_emails: Optional[List[str]] = None):
    try:
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        } 

        to_receipients = [{"emailAddress": {"address": email}} for email in receipient_emails]
        cc_receipients = [{"emailAddress": {"address": email}} for email in cc_emails] if cc_emails else []

        email_message = {
            "message": {
                "subject": subject,
                "body": {
                    "contentType": "HTML",
                    "content": html_body
                },
                "toRecipients": to_receipients,
                "ccRecipients": cc_receipients
            },
            "saveToSentItems": True
        }

        if cc_receipients:
            email_message['message']['ccRecipients'] = cc_receipients

        send_url = f"https://graph.microsoft.com/v1.0/users/{sender_email}/sendMail"

        logger.info(f"Sending email via Graph to {len(receipient_emails)} recipients")
        logger.debug(f"Subject: {subject}")

        respone = requests.post(send_url, headers=headers, json=email_message) 

        if respone.status_code == 202:
            logger.info("Email sent successfully")
            return True
        else:
            logger.error(f"Failed to send email: {respone.text}")
            return False 
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False    


def send_bcp_notification(tenant_id: str, client_id: str, client_secret: str, sender_email: str, receipient_emails: List[str], date_str: str, areas_total: int, areas_success: int, areas_failed: int, employees_total: int, employees_success: int, employees_failed: int, cc_emails: Optional[List[str]] = None, smtp_server: Optional[str] = None, smtp_port: Optional[int] = None, smtp_password: Optional[str] = None, use_smtp_fallback: bool = True):

    subject = f"BCP PDF Generation Report - {date_str}"

    html_body = create_email_html_body(
        date_str,
        areas_total,
        areas_success,
        areas_failed,
        employees_total,
        employees_success,
        employees_failed
    )     


    try:
        logger.info("Attempting to send email via Graph API")
        access_token = get_graph_access_token(tenant_id,client_id, client_secret)

        success = send_email_via_graph(
            access_token,
            sender_email,
            receipient_emails,
            subject,
            html_body,
            cc_emails
        )

        if success:
            return True

        logger.warning("Microsoft Graph API failed, attempting SMTP fallback")

    except Exception as e:
        logger.warning(f"Microsoft Graph API failed: {e}")
        logger.info("Attempting SMTP fallback")

    if use_smtp_fallback and smtp_server and smtp_port and smtp_password:
        try: 
            return send_email_via_smtp(
                smtp_server,
                smtp_port,
                sender_email,
                smtp_password,
                receipient_emails,
                subject,
                html_body,
                cc_emails
            )
        except Exception as e:
            logger.error(f"Failed to send email via SMTP: {e}")
            return False
    else:
        if use_smtp_fallback:
            logger.error("SMTP fallback not configured missing required parameters (smtp_server, smtp_port, smtp_password)")
        else:
            logger.warning("SMTP fallback not enabled")
            return False