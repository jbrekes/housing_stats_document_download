# Housing Stats Document Download

This Python script automates the process of downloading the most up-to-date SQLite database from the [Swiss Federal Statistical Office (OFS)](https://www.housing-stat.ch/fr/madd/public.htm) and updates specific fields on Google Cloud Storage. The downloaded files are used as sources for some BigQuery tables.

## Features

- Downloads the latest SQLite database from OFS website.
- Extracts relevant tables from the downloaded ZIP file.
- Reads SQLite tables into Pandas DataFrames.
- Converts DataFrames to Parquet format.
- Uploads Parquet files to Google Cloud Storage.

## Prerequisites

- Python 3 installed on your machine.
- Pip package manager.
- Google Cloud Storage account.
- Google Cloud project with appropriate permissions.
- Service account key file for accessing Google Cloud Storage.

## Setup

1. Clone this repository:

```bash
git clone https://github.com/jbrekes/housing_stats_document_download.git
```

2. Navigate to the project directory:

```bash
cd housing_stats_document_download
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up a `.env` file in the project directory with the following environment variables:
```plaintext
credentials_path=/path/to/your/service_account_key.json
project_id=your-google-cloud-project-id
bucket_name=your-google-cloud-storage-bucket-name
```

5. Run the script:
```bash
python housing_stats.py
```

Feel free to customize it further based on your specific requirements or additional information you want to provide.
