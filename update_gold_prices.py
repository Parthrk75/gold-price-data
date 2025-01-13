import os
import pandas as pd
import yfinance as yf
import requests
import base64
import schedule
import time
from datetime import datetime

# GitHub repository details
GITHUB_REPO = 'Parthrk75/gold-price-data'
CSV_FILE_PATH = 'historical_gold_spot_prices.csv'
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')  # Ensure token is set as an environment variable

# Retry function for network calls
def retry(func, retries=3, delay=5):
    for i in range(retries):
        try:
            return func()
        except Exception as e:
            print(f"Retry {i + 1}/{retries} failed: {e}")
            time.sleep(delay)
    print("Max retries reached. Exiting.")
    return None

# Function to fetch historical data from Yahoo Finance
def fetch_data_from_yfinance(ticker, start_date, end_date):
    try:
        gld = yf.Ticker(ticker)
        return gld.history(start=start_date, end=end_date)
    except Exception as e:
        print(f"Error fetching data from Yahoo Finance: {e}")
        return None

# Function to update the CSV file with the latest data
def update_csv_file():
    if not GITHUB_TOKEN:
        print("Error: GitHub token is missing. Set it as an environment variable.")
        return

    today = datetime.today().strftime('%Y-%m-%d')
    start_date = "2020-01-01"  # Default start date if no data exists

    # Check if CSV file exists and read existing data
    if os.path.exists(CSV_FILE_PATH):
        try:
            existing_data = pd.read_csv(CSV_FILE_PATH, on_bad_lines='skip')
            existing_data.columns = ["Date", "Open (Spot Price USD)", "High (Spot Price USD)", 
                                     "Low (Spot Price USD)", "Close (Spot Price USD)"]

            existing_data["Date"] = pd.to_datetime(existing_data["Date"], errors='coerce')
            existing_data = existing_data.dropna(subset=["Date"])

            last_date = existing_data["Date"].max().strftime('%Y-%m-%d')
            if last_date == today:
                print(f"Data for {today} already exists. Skipping update.")
                return
            start_date = (pd.to_datetime(last_date) + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
            print(f"Fetching new data starting from: {start_date}")
        except pd.errors.ParserError:
            print("Error reading CSV file due to inconsistent data. Skipping...")
            return

    # Fetch new data from Yahoo Finance
    historical_data = fetch_data_from_yfinance("GLD", start_date, today)
    if historical_data is None or historical_data.empty:
        print("No new data fetched from Yahoo Finance.")
        return

    # Process and update CSV
    try:
        selected_data = historical_data[["Open", "High", "Low", "Close"]]
        scaling_factor = 10.77
        selected_data["Open (Spot Price USD)"] = selected_data["Open"] * scaling_factor
        selected_data["High (Spot Price USD)"] = selected_data["High"] * scaling_factor
        selected_data["Low (Spot Price USD)"] = selected_data["Low"] * scaling_factor
        selected_data["Close (Spot Price USD)"] = selected_data["Close"] * scaling_factor
        selected_data = selected_data.reset_index()
        selected_data["Date"] = selected_data["Date"].dt.strftime('%Y-%m-%d')

        if os.path.exists(CSV_FILE_PATH):
            existing_dates = existing_data["Date"].dt.strftime('%Y-%m-%d').tolist()
            new_data = selected_data[~selected_data["Date"].isin(existing_dates)]
        else:
            new_data = selected_data

        if not new_data.empty:
            if os.path.exists(CSV_FILE_PATH):
                new_data.to_csv(CSV_FILE_PATH, mode='a', header=False, index=False)
            else:
                new_data.to_csv(CSV_FILE_PATH, index=False)
            print(f"New data added for {len(new_data)} days.")
        else:
            print("No new data to add. All data already exists in the CSV file.")
    except Exception as e:
        print(f"Error updating CSV file: {e}")

# Function to push the updated CSV file to GitHub
def push_to_github():
    if not GITHUB_TOKEN:
        print("Error: GitHub token is missing. Set it as an environment variable.")
        return

    try:
        commit_message = f"Update gold price data: {datetime.today().strftime('%Y-%m-%d')}"
        with open(CSV_FILE_PATH, 'rb') as file:
            content = file.read()

        url = f'https://api.github.com/repos/{GITHUB_REPO}/contents/{CSV_FILE_PATH}'
        headers = {'Authorization': f'token {GITHUB_TOKEN}'}
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Error fetching file metadata: {response.status_code}, {response.json()}")
            return

        content_data = response.json()
        sha = content_data.get('sha', None)
        encoded_content = base64.b64encode(content).decode('utf-8')

        payload = {
            'message': commit_message,
            'content': encoded_content,
            'sha': sha
        }
        response = requests.put(url, headers=headers, json=payload)
        if response.status_code in [200, 201]:
            print("Successfully updated the CSV file on GitHub.")
        else:
            print(f"Error pushing to GitHub: {response.status_code}, {response.json()}")
    except Exception as e:
        print(f"Error during GitHub push: {e}")

# Main function to schedule the job
def main():
    schedule.every().day.at("00:00").do(lambda: (update_csv_file(), push_to_github()))
    print("Scheduler started. Waiting for the next job...")
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    main()
