#Libraries
import os
import re
import json
import requests
import numpy as np
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
# from datetime import datetime as dt, timedelta as td
import datetime as dt 

load_dotenv()

st.title("TML Sanitization check")

# Read the date from the text file
# try:
#     with open('latest_date.txt', 'r') as file:
#         saved_date = file.read().strip()
#         st.write(f"Last file was downloaded on : {saved_date}")
        
# except FileNotFoundError:
#     st.write("The file 'latest_date.txt' does not exist.")

today = dt.datetime.now()
last_year = today.year - 1
jan_1 = dt.date(last_year, 1, 1)
dec_31 = dt.date(today.year, 12, 31)

#date selection, env selector - drop down, submit, download button for csv file
env = st.selectbox(
    "Choose env for analysis",
    ("Staging", "Prod"),
)

if env == "Staging":
    authority = os.getenv("staging_authority")
    referer = os.getenv("staging_referer")
    url = os.getenv("staging_url")
else:
    authority = os.getenv("prod_authority")
    referer = os.getenv("prod_referer")
    url = os.getenv("prod_url")
    

date_range = st.date_input("Select date range ",
                  (jan_1, jan_1),
                  jan_1,
                  dec_31,
                  format="DD.MM.YYYY",
                  )
    
#### TML sanitization comparison
OPERATION_FOR_AS_SEARCH = '/convassist.ConvAssistGrpcService/SendMessage'

#### Get Traces from Jaeger
# Get the traces from Jaeger for the given operation name for the given day using Jaeger REST API
def get_trace(service: str, operation: str, start, end, limit: int = 100):
    headers = {
    # 'authority': 'eureka.thoughtspotstaging.cloud',
    # 'authority': 'eureka.thoughtspot.cloud',
    'authority': authority ,
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'referer': referer,
    # 'referer': 'https://eureka.thoughtspotstaging.cloud/tracing/search',
    # 'referer': 'https://eureka.thoughtspot.cloud/tracing/search',
    'sec-ch-ua': '"Not(A:Brand";v="24", "Chromium";v="122"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
}

#change url to - 'https://eureka.thoughtspot.cloud/tracing/api/traces', for prod
#change url to - 'https://eureka.thoughtspotstaging.cloud/tracing/api/traces', for staging
    response = requests.get( url,
        params={
            'lookback': 'custom',
            'start': start.strftime('%s') + '000000',
            'end': end.strftime('%s') + '000000',
            'limit': limit,
            'maxDuration': '',
            'minDuration': '',
            'operation': operation,
            'service': service,
        },
        headers=headers,
        timeout=60 * 3,
    )

    return response.json()

# With formula info
# Function to extract TML information (sanitizeTMLQuery and getResultFromTML)
def get_sanitize_TML_withFormulas(file_path):
    """_summary_

    Args:
        file_path (_type_): _description_

    Returns:
        _type_: _description_
    """
    extracted_data = []
    with open(file_path, 'r') as f:
        json_data = json.load(f)
        for j_data in json_data['data']:
            spans = j_data.get('spans', [])
            for span in spans:
                trace_id = span.get('traceID')
                logs = span.get('logs', [])
                operation_name = span.get('operationName')

                # Initialize default values for TML Before, After, and Formula Info
                tml_before = None
                tml_after = None
                formula_info = None
                timestamp = None

                # If operation is "sanitizeTMLQuery", extract TML before and after sanitization
                if operation_name == 'sanitizeTMLQuery':
                    for log in logs:
                        fields = log.get('fields', [])
                        for field in fields:
                            value = field.get('value', '')

                            if "TML query Before Sanitization" in value:
                                tml_before = re.sub(r'TML query Before Sanitization: ', '', value)
                                timestamp = log.get('timestamp')
                            elif "TML query After Sanitization" in value:
                                tml_after = re.sub(r'TML query After Sanitization: ', '', value)

                # If operation is "getResultFromTML", extract formula information
                elif operation_name == 'getResultFromTML':
                    for log in logs:
                        fields = log.get('fields', [])
                        for field in fields:
                            value = field.get('value', '')
                            
                            # Match and extract formula information
                            if "formulas {" in value:
                                formula_info = extract_formulas(value)

                # Append the parsed data to the list
                if tml_before or tml_after or formula_info:
                    extracted_data.append({
                        'traceID': trace_id,
                        'timestamp': timestamp,
                        'TML Before Sanitization': tml_before,
                        'TML After Sanitization': tml_after,
                        'Formula Info': formula_info
                    })

    return extracted_data
       

# Function to extract formulas from the getResultFromTML log entry
def extract_formulas(log_value):
    formulas = []
    formula_pattern = re.compile(r'formulas\s*{\s*name:\s*"([^"]+)"\s*expression:\s*"([^"]+)"', re.DOTALL)

    # Find all formulas using the regex pattern
    for match in formula_pattern.finditer(log_value):
        name = match.group(1).strip()
        expression = match.group(2).strip()
        formulas.append({'name': name, 'expression': expression})

    return formulas if formulas else None


# Loop through all json files and collect results
def process_all_json_files(directory):
    all_extracted_data = []
    # Show progress for processing files
    total_files = len(os.listdir(directory))
    progress_bar = st.progress(0)  # Initialize progress bar
    status_text = st.empty()       # Placeholder for status text
    # for idx, filename in enumerate(os.listdir(directory)):
    #     file_path = os.path.join(directory, filename)
    #     process_file(file_path)  # Your file processing logic
    #     progress_bar.progress((idx + 1) / total_files)
    #     status_text.text(f"Processing file {idx + 1} of {total_files}")
    # List all files in the directory
    for idx, filename in enumerate(os.listdir(directory)):
        if filename.endswith(".json"):  # Process only JSON files
            file_path = os.path.join(directory, filename)
            # st.write(f'Processing file: {file_path}')
            extracted_data = get_sanitize_TML_withFormulas(file_path)
            all_extracted_data.extend(extracted_data)
        # progress_bar.progress((idx + 1) / total_files)
        # status_text.text(f"Processing file {idx + 1} of {total_files}")

    # Create a DataFrame from the collected data
    df = pd.DataFrame(all_extracted_data)
    
    return df

#### Compare TMLs 
# # Function to tokenize the TML queries
def tokenize_tml(tml_string):
    if tml_string is None:
        return []
    # Extract words (letters/numbers) and TML elements, ignoring symbols/punctuation
    # tokens = re.findall(r'\w+|\S', tml_string)
    tokens = re.findall(r'\b\w+\b', tml_string.lower())
    return tokens

# Function to compare TML before and after sanitization
def compare_tmls(df):
    # Lists to store the results
    dropped_list = []
    tokens_dropped_list = []
    
    for index, row in df.iterrows():
        tml_before = row['TML Before Sanitization']
        tml_after = row['TML After Sanitization']
        
        # Tokenize both TMLs
        tokens_before = set(tokenize_tml(tml_before))
        tokens_after = set(tokenize_tml(tml_after))
        
        # Compare tokens and find the dropped ones
        tokens_dropped = tokens_before - tokens_after
        num_dropped = len(tokens_dropped)
        
        if num_dropped > 0:
            dropped_list.append(num_dropped)
            tokens_dropped_list.append(list(tokens_dropped))
        else:
            dropped_list.append(0)
            tokens_dropped_list.append("NA")
    
    # Add the results to the DataFrame
    df['dropped/not'] = dropped_list
    df['tokens dropped'] = tokens_dropped_list

    return df

@st.cache_data
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode("utf-8")
    
if __name__ == "__main__":
    # Ensure the script runs only when both selections are made
    if st.button("Run Analysis"):
        if env == "Select":
            st.warning("Please select an environment!")
        elif not date_range or len(date_range) < 2:
            st.warning("Please select a valid date range!")
        else:
            # Assign environment-specific variables
            if env == "Staging":
                authority = os.getenv("staging_authority")
                referer = os.getenv("staging_referer")
                url = os.getenv("staging_url")
            else:
                authority = os.getenv("prod_authority")
                referer = os.getenv("prod_referer")
                url = os.getenv("prod_url")

            start_date, end_date = date_range

            st.write(f"Environment: {env}")
            st.write(f"Date range: {start_date} to {end_date}")
            
            # Call your functions here to fetch and process data
            st.write("Running the analysis...")
        # Assuming the last download timestamp is provided
        # last_downloaded_ts = dt.datetime.fromisoformat(saved_date)#(2024, 12, 31, 8, 00, 0)
        # Get current time
        now = dt.datetime.combine(end_date, dt.time.min) #dt.datetime(2024, 12, 31, 11, 00, 0)#datetime.now()

        # Function to fetch trace data
        def get_trace_data(start_time, end_time):
            # st.write(f'Fetching trace data from {start_time} to {end_time}')
            trace_data = get_trace('convassist', OPERATION_FOR_AS_SEARCH, start_time, end_time, 900)
            return trace_data

        # Loop through each hour in the past 7 days, moving hour by hour
        current_time = dt.datetime.combine(start_date, dt.time.min) #last_downloaded_ts
        with st.spinner('Fetching trace data...'):
            while current_time <= now and (now - current_time).days <= 7:
                # Calculate endtime, which is 1 hour after the current_time
                endtime = current_time + dt.timedelta(hours=1)                
                # Fetch trace data for the current hour
                trace_data = get_trace_data(current_time, endtime)
                os.makedirs(f'data/{env}/traces_{now.date()}', exist_ok=True)
                new_dir = f'data/{env}/traces_{now.date()}'
                json_filename = f'{new_dir}/query-{current_time.strftime("%Y-%m-%d")}-{current_time.hour}.json'
                with open(json_filename, 'w') as f:
                    json.dump(trace_data, f)
                    print(f'Saved trace data for {current_time} to {json_filename}')

                # Move to the next hour
                current_time = endtime
            # st.success('Trace data fetched!')
            
        # directory = f'data/staging/traces_{now.date()}'  # staging directory path containing JSON files
        directory = f'data/{env}/traces_{now.date()}'  # prod directory path containing JSON files
        
        # Process all files and get the data into a DataFrame
        df = process_all_json_files(directory)
        # st.write("DataFrame columns:", df.columns)
        # st.write("Sample data from JSON:", df.head())
        if 'traceID' in df.columns:
            st.success('Trace data fetched!')
            df = df.groupby('traceID', as_index=False).agg({
            'timestamp': 'first',  # Keep the first non-null value
            'TML Before Sanitization': 'first',
            'TML After Sanitization': 'first',
            'Formula Info': 'first'
            })
            # Compare the TMLs and add the new columns
            df = compare_tmls(df)   
            
            # Save the DataFrame to a CSV file
            # df.to_csv(f'data/staging_result/result_{now.date()}.csv', index=False)
            df.to_csv(f'results/{env}/result_{now.date()}.csv', index=False)
            st.write(f'CSV file saved as result_{now.date()}.csv')
            st.write('TML Sanitization result')
            st.write(df.head()) 
            
            csv = convert_df(df)
            st.download_button(
                label="Download data as CSV",
                data=csv,
                file_name=f"TML Sanitization result {now.date()}.csv",
                mime="text/csv",
            )
        else:
            # st.error("Column 'traceID' is missing from the DataFrame.")
            st.error("No traces available for this Time Period")
        # df.to_csv('extracted_data_new.csv', index=False)
        