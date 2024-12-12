# File_batch_processing

#### A global company handles sales data generated in multiple legacy systems distributed across different time zones. Each system sends daily batch files in various formats (CSV, JSON, and XML) to a central system, considering that the files should reside in a folder called "raw." This repository intends to process and analyze the data by joining the files of the raw folder, and obtaining relevant statistics from it.

Run all commands from root directory C:\...\File_batch_processing

## Prerequisites
- Python Installed
- Clone repository

## For Windows
1. Download dependencies:
```bash
python -m venv venv      # Create venv
.\venv\Scripts\activate    # Activate venv
pip install -r requirements.txt   # Install
```
2. Run
```bash
python BatchProcessor/BatchProcessor.py     # Create files
```

## For Linux/Mac
1. Download dependencies:
```bash
python3 -m venv venv    # Create venv
source venv/bin/activate   # Activate venv
pip install -r requirements.txt   # Install
```
2. Run
```bash
python3 BatchProcessor/BatchProcessor.py     # Create files
```