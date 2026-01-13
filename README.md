# Azure Serverless Hospital Analytics

![Python](https://img.shields.io/badge/python-3.9+-blue.svg)
![Azure](https://img.shields.io/badge/azure-%230072C6.svg?style=flat&logo=microsoftazure&logoColor=white)
![Azure Functions](https://img.shields.io/badge/Azure%20Functions-Lightning-yellow)
![SQLite](https://img.shields.io/badge/sqlite-%2307405e.svg?style=flat&logo=sqlite&logoColor=white)
![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?style=flat&logo=pandas&logoColor=white)
![License](https://img.shields.io/badge/license-MIT-green.svg)

> [Polska wersja dokumentacji / Polish version here](README.pl.md)

Serverless system built with **Azure Functions** that simulates hospital operations, persists state using SQLite on Blob Storage, and performs daily data analytics with automated email reporting.

## Key Features

* **Stateful Serverless:** Demonstrates how to handle state in a stateless environment (Azure Functions) using the "check-out/check-in" pattern with SQLite and Blob Storage.
* **Data Simulation:** Generates realistic patient data (PESEL, age, gender) and medical events (admissions, discharges, deaths) using `Faker`.
* **Analytics Pipeline:** Detects prolonged stays (above 90th percentile) and readmissions (< 14 days).
* **Reporting:** Generates Matplotlib/Seaborn visualizations and sends them via SMTP email.

## Tech Stack

* **Cloud:** Azure Functions (Consumption Plan), Blob Storage
* **Core:** Python 3.9+
* **Data:** Pandas, NumPy, SQLite3
* **Visualization:** Matplotlib, Seaborn
* **Utils:** Faker, smtplib

## Architecture

The system relies on two Time Trigger functions:

1.  **DailyGenerator (02:00 AM):** Downloads the database, simulates a full day of hospital events (new patients, updates), and uploads the DB back to storage.
2.  **DailyAnalysis (04:00 AM):** Downloads the DB, calculates statistics, generates PNG charts, and emails the report.

## Setup & Installation

1.  **Clone the repository**
    ```bash
    git clone [https://github.com/YOUR_USERNAME/Azure-Hospital-Analytics.git](https://github.com/YOUR_USERNAME/Azure-Hospital-Analytics.git)
    cd Azure-Hospital-Analytics
    ```

2.  **Create Virtual Environment**
    ```bash
    python -m venv .venv
    # Windows:
    .venv\Scripts\activate
    # Linux/Mac:
    source .venv/bin/activate
    ```

3.  **Install Dependencies**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configuration**
    Rename `local.settings.json.example` to `local.settings.json` and update it with your credentials:
    * `AzureWebJobsStorage`: Connection string to your Azure Storage Account.
    * `EMAIL_*`: SMTP configuration for sending reports.

5.  **Run Locally**
    ```bash
    func start
    ```

## License

This project is licensed under the MIT License.
