# MAILDASH

MailDash is a web-based dashboard and automation tool designed to extract data from recurring email attachments and visualize it.

It works by connecting to a local Outlook client, finding specific report emails (based on sender, client, and region), downloading their Excel/CSV attachments, and ingesting the data into an SQLite database for monitoring and analysis.

## Core Features

* **Automated Email Polling:** Uses PowerShell scripts to interface with a local Outlook client and find specific emails.
* **Data Ingestion:** Reads data from Excel (`.xls*`) and `.csv` attachments using `pandas` and saves the data to a central SQLite database (`analytics.db`).
* **Web Dashboard:** A FastAPI backend serves a plain HTML/CSS/JavaScript frontend with several key pages:
    * **Overview:** Manually trigger an email fetch and data ingest.
    * **Excel Viewer:** Load and view the raw data from the most recently downloaded spreadsheet.
    * **Stats Page:** Visualize historical data from the database using Chart.js.
    * **Live Display:** A monitoring screen designed to auto-cycle through the latest data for different client/workspace combinations.
    * **Admin Panel:** A password-protected page to add, remove, or update the Clients, Regions, and Sender Email addresses that the application monitors.

## Technical Stack

* **Backend:** FastAPI (Python)
* **Server:** Uvicorn
* **Data Processing:** Pandas
* **Database:** SQLite
* **Frontend:** HTML, CSS, JavaScript (no framework)
* **Core Dependency:** Local Windows Outlook Client & PowerShell (for MAPI access)

## Setup and Installation

1.  **Prerequisite:** This application requires a Windows machine with a local Outlook client installed and configured.

2.  Clone this repository or ensure your project folder is set up.

3.  **Create a Virtual Environment:**
    ```bash
    python -m venv venv
    ```

4.  **Activate the Environment:**
    * On Windows (Command Prompt / PowerShell):
        ```bash
        .\venv\Scripts\activate
        ```
    * On macOS / Linux:
        ```bash
        source venv/bin/activate
        ```

5.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

## Running the Application

This project uses a nested directory structure (`backend/app/main.py`). You must run the server from the **root `MAILDASH` directory**.

1.  **Run the Uvicorn Server:**
    ```bash
    uvicorn backend.app.main:app --host 127.0.0.1 --port 8080 --reload
    ```
    * `--port 8080`: Specifies port 8080. You can change this to `8000` or any other free port.
    * `--reload`: Enables auto-reload when you make code changes.

2.  **Open the Dashboard:**
    Open your web browser and navigate to:
    **`http://localhost:8080`**

    You will be redirected to the `/dashboard` page.