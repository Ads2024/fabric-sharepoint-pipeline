# Fabric PDF Generator

A robust Python application for automating the generation of PDFs from PowerBI reports, uploading them to SharePoint, and sending email notifications.

## Features
-   **PowerBI Integration**: Exports reports to PDF using PowerBI REST APIs.
-   **SharePoint Integration**: Uploads generated files and creates shareable links.
-   **Fabric Lakehouse**: dynamic data querying.
-   **Notifications**: Automated email reporting via Microsoft Graph or SMTP.
-   **Enterprise Ready**: CI/CD pipelines, generic data handling, and robust logging.

## Prerequisites
-   Python 3.10+
-   Azure Service Principal with access to:
    -   PowerBI Workspace
    -   Fabric Lakehouse (SQL Endpoint)
    -   SharePoint Site
    -   Microsoft Graph (Mail.Send)

## Installation

1.  **Clone the repository**:
    ```bash
    git clone <repository-url>
    cd fabric-pdf-generatior
    ```

2.  **Create and activate a virtual environment**:
    ```bash
    python -m venv venv
    # Windows
    venv\Scripts\activate
    # Linux/Mac
    source venv/bin/activate
    ```

3.  **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

## Configuration

1.  **Environment Variables**: Create a `.env` file or set the following variables:
    -   `TENANT_ID`
    -   `CLIENT_ID`
    -   `CLIENT_SECRET`
    -   `POWERBI_WORKSPACE_ID`
    -   `POWERBI_REPORT_ID`
    -   `FABRIC_SQL_ENDPOINT`
    -   `FABRIC_DATABASE`
    -   `SHAREPOINT_TENANT_ID`, `SHAREPOINT_CLIENT_ID`, `SHAREPOINT_CLIENT_SECRET`
    -   `EMAIL_SENDER`, `EMAIL_RECIPIENTS`

2.  **Config File**: Edit `config/config.yaml` to define:
    -   SQL Queries (`queries.areas`, `queries.employees`)
    -   SharePoint folder paths
    -   Batch processing sizes

## Usage

To run the full workflow:
```bash
python src/main.py --config config/config.yaml
```

**Options**:
-   `--dry-run`: Simulate execution without external calls.
-   `--report-type`: Choose `areas`, `employees`, or `both`.

## CI/CD Configuration (GitHub Actions)
The repository includes a GitHub Actions workflow (`.github/workflows/ci.yml`) that runs:
1.  **Tests**: On every push/PR to `main`.
2.  **Production Sync**: Daily at 8:00 AM Sydney Time (or via manual trigger).

**Required GitHub Secrets**:
To enable the production workflow, add the following as Repository Secrets:
-   `TENANT_ID`, `CLIENT_ID`, `CLIENT_SECRET`
-   `POWERBI_WORKSPACE_ID`, `POWERBI_REPORT_ID`
-   `FABRIC_SQL_ENDPOINT`, `FABRIC_DATABASE`
-   `SHAREPOINT_TENANT_ID`, `SHAREPOINT_CLIENT_ID`, `SHAREPOINT_CLIENT_SECRET`
-   `EMAIL_SENDER`, `EMAIL_RECIPIENTS`
-   `SMTP_SERVER`, `SMTP_PORT`, `SMTP_PASSWORD`

## Development

**Running Tests**:
```bash
python -m unittest discover tests
```

**Project Structure**:
-   `src/`: Source code
-   `config/`: Configuration files
-   `tests/`: Unit tests