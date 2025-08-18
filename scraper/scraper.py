"""
Brazil Oil & Gas Production Daily Scraper

A complete scraper that:
1. Uses Selenium to navigate and configure PowerBI dashboard
2. Makes API request to get data  
3. Processes JSON response using binary mask logic and converts to CSV format

Project: brazil_oil_gas_production_daily - 17065
"""

import os
import time
import json
import sys
import csv
import requests
from datetime import datetime, timedelta
from typing import Any, Dict, List, Union

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options


def setup_chrome_driver():
    """Setup Chrome WebDriver with appropriate options for PowerBI scraping."""
    chrome_options = Options()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--allow-insecure-localhost')
    chrome_options.add_argument('--disable-web-security')
    # chrome_options.add_argument('--headless')  # Uncomment for headless mode
    
    return webdriver.Chrome(options=chrome_options)


def navigate_powerbi_dashboard(historical: bool = False):
    """
    Step 1: Navigate PowerBI dashboard and configure filters.
    
    Args:
        historical: If True, scrape from 2000 to now; if False, last 14 days
        
    Returns:
        tuple: (start_date, end_date) in MM/dd/YYYY format
    """
    print("Step 1: Navigating PowerBI dashboard and configuring filters...")
    
    driver = setup_chrome_driver()
    
    try:
        wait = WebDriverWait(driver, 40)
        url = "https://app.powerbi.com/view?r=eyJrIjoiZjQ0NjIzNmYtNzY3Ni00MzZkLWI0MTQtYzk4ZWY0ZGI4ODQ5IiwidCI6IjQ0OTlmNGZmLTI0YTYtNGI0Mi1iN2VmLTEyNGFmY2FkYzkxMyJ9"
        driver.get(url)
        time.sleep(8)

        # Click the 'Instalações' imageBackground
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "imageBackground")))
        for element in driver.find_elements(By.CLASS_NAME, "imageBackground"):
            style = element.get_attribute("style")
            if "width: 190.229px" in style and "height: 40.9429px" in style:
                element.click()
                break
        time.sleep(8)

        # Set date range
        if historical:
            start_date = "01/01/2025"
            end_date = datetime.now().strftime("%m/%d/%Y")
        else:
            start_date = (datetime.now() - timedelta(days=13)).strftime("%m/%d/%Y")
            end_date = datetime.now().strftime("%m/%d/%Y")

        # Configure date inputs
        try:
            end_date_input = wait.until(EC.presence_of_element_located(
                (By.XPATH, "//input[contains(@aria-label, 'End date')]")
            ))
            end_date_input.clear()
            end_date_input.send_keys(end_date + "\n")
            
            try:
                start_date_input = driver.find_element(By.XPATH, "//input[contains(@aria-label, 'Start date')]")
                start_date_input.clear()
                start_date_input.send_keys(start_date + "\n")
            except Exception:
                pass
        except Exception:
            pass
        time.sleep(8)

        # Expand 'Produção por Instalação'
        try:
            table_header = wait.until(EC.element_to_be_clickable((By.XPATH, "//h3[text()='Produção por Instalação']")))
            driver.execute_script("arguments[0].click();", table_header)
        except Exception:
            pass
        time.sleep(5)

        # Click Focus mode
        try:
            focus_btn = wait.until(EC.element_to_be_clickable(
                (By.XPATH, '//button[contains(@aria-label, "Focus mode")]')
            ))
            driver.execute_script("arguments[0].click();", focus_btn)
        except Exception:
            pass
        time.sleep(10)

        print(f"   Date range configured: {start_date} to {end_date}")
        return start_date, end_date

    finally:
        driver.quit()


def convert_date_format(date_str: str) -> str:
    """Convert date from MM/dd/YYYY to YYYY-MM-dd format for API"""
    try:
        from datetime import datetime
        date_obj = datetime.strptime(date_str, "%m/%d/%Y")
        return date_obj.strftime("%Y-%m-%d")
    except ValueError:
        return date_str


def make_powerbi_api_request(start_date: str, end_date: str):
    """
    Step 2: Make API request to PowerBI to get the data.
    
    Args:
        start_date: Start date in MM/dd/YYYY format
        end_date: End date in MM/dd/YYYY format
        
    Returns:
        dict: JSON response from PowerBI API
    """
    print("Step 2: Making PowerBI API request...")
    
    # Convert dates to YYYY-MM-dd format for the API
    api_start_date = convert_date_format(start_date)
    api_end_date = convert_date_format(end_date)
    
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Content-Type': 'application/json;charset=UTF-8',
        'Origin': 'https://app.powerbi.com',
        'Referer': 'https://app.powerbi.com/',
        'User-Agent': 'Mozilla/5.0',
        'X-PowerBI-ResourceKey': 'f446236f-7676-436d-b414-c98ef4db8849'
    }
    
    params = {'synchronous': 'true'}
    
    json_data = {
        'version': '1.0.0',
        'queries': [
            {
                'Query': {
                    'Commands': [
                        {
                            'SemanticQueryDataShapeCommand': {
                                'Query': {
                                    'Version': 2,
                                    'From': [
                                        {'Name': 'd', 'Entity': 'Datas', 'Type': 0},
                                        {'Name': 'v', 'Entity': 'v_instalacoes_final', 'Type': 0},
                                        {'Name': 'm', 'Entity': 'Medidas', 'Type': 0},
                                        {'Name': 'c', 'Entity': 'Correção', 'Type': 0},
                                    ],
                                    'Select': [
                                        {'Column': {'Expression': {'SourceRef': {'Source': 'd'}}, 'Property': 'Data'}, 'Name': 'Datas.Data'},
                                        {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Instalação'}, 'Name': 'v_instalacoes_final.Instalação'},
                                        {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Operadora'}, 'Name': 'v_instalacoes_final.Operadora'},
                                        {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Tipo'}, 'Name': 'v_instalacoes_final.Tipo'},
                                        {'Measure': {'Expression': {'SourceRef': {'Source': 'm'}}, 'Property': 'Petróleo'}, 'Name': 'Medidas.Petroleo'},
                                        {'Measure': {'Expression': {'SourceRef': {'Source': 'm'}}, 'Property': 'Gás Mm3'}, 'Name': 'Medidas.Gás'},
                                        {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Estado'}, 'Name': 'v_instalacoes_final.Estado'},
                                        {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Cidade'}, 'Name': 'v_instalacoes_final.Cidade'},
                                        {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Atendimento Campo'}, 'Name': 'v_instalacoes_final.Atendimento Campo'},
                                        {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Campo'}, 'Name': 'v_instalacoes_final.Campo'},
                                        {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'IND_ATIVO'}, 'Name': 'v_instalacoes_final.IND_ATIVO'},
                                        {'Aggregation': {'Expression': {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'qtde_pms'}}, 'Function': 0}, 'Name': 'Sum(v_instalacoes_final.qtde_pms)'},
                                        {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Silga'}, 'Name': 'v_instalacoes_final.Silga'},
                                        {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Situação'}, 'Name': 'v_instalacoes_final.Situação'},
                                        {'Measure': {'Expression': {'SourceRef': {'Source': 'm'}}, 'Property': 'Petróleo Equivalente boe'}, 'Name': 'Medidas.Petroleo Equivalente boe'},
                                    ],
                                    'Where': [
                                        {
                                            'Condition': {
                                                'Not': {
                                                    'Expression': {
                                                        'Comparison': {
                                                            'ComparisonKind': 0,
                                                            'Left': {
                                                                'Measure': {
                                                                    'Expression': {'SourceRef': {'Source': 'm'}},
                                                                    'Property': 'Petróleo Equivalente boe',
                                                                },
                                                            },
                                                            'Right': {'Literal': {'Value': 'null'}},
                                                        },
                                                    },
                                                },
                                            },
                                            'Target': [
                                                {'Column': {'Expression': {'SourceRef': {'Source': 'd'}}, 'Property': 'Data'}},
                                                {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Instalação'}},
                                                {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Operadora'}},
                                                {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Tipo'}},
                                                {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Estado'}},
                                                {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Cidade'}},
                                                {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Atendimento Campo'}},
                                                {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Campo'}},
                                                {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'IND_ATIVO'}},
                                                {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Silga'}},
                                                {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Situação'}},
                                            ],
                                        },
                                        {
                                            'Condition': {
                                                'In': {
                                                    'Expressions': [
                                                        {
                                                            'Column': {
                                                                'Expression': {
                                                                    'SourceRef': {
                                                                        'Source': 'c',
                                                                    },
                                                                },
                                                                'Property': 'Unidade',
                                                            },
                                                        },
                                                    ],
                                                    'Values': [
                                                        [
                                                            {
                                                                'Literal': {
                                                                    'Value': "'bbl'"
                                                                },
                                                            },
                                                        ],
                                                    ],
                                                },
                                            },
                                        },
                                        {
                                            'Condition': {
                                                'And': {
                                                    'Left': {
                                                        'Comparison': {
                                                            'ComparisonKind': 2,
                                                            'Left': {
                                                                'Column': {
                                                                    'Expression': {
                                                                        'SourceRef': {
                                                                            'Source': 'd',
                                                                        },
                                                                    },
                                                                    'Property': 'Data',
                                                                },
                                                            },
                                                            'Right': {
                                                                'Literal': {
                                                                    'Value': f"datetime'{api_start_date}T00:00:00'"
                                                                },
                                                            },
                                                        },
                                                    },
                                                    'Right': {
                                                        'Comparison': {
                                                            'ComparisonKind': 3,
                                                            'Left': {
                                                                'Column': {
                                                                    'Expression': {
                                                                        'SourceRef': {
                                                                            'Source': 'd',
                                                                        },
                                                                    },
                                                                    'Property': 'Data',
                                                                },
                                                            },
                                                            'Right': {
                                                                'Literal': {
                                                                    'Value': f"datetime'{api_end_date}T00:00:00'"
                                                                },
                                                            },
                                                        },
                                                    },
                                                },
                                            },
                                        },
                                    ],
                                    'OrderBy': [
                                        {
                                            'Direction': 2,
                                            'Expression': {
                                                'Column': {
                                                    'Expression': {
                                                        'SourceRef': {
                                                            'Source': 'd',
                                                        },
                                                    },
                                                    'Property': 'Data',
                                                },
                                            },
                                        },
                                    ],
                                },
                                'Binding': {
                                    'Primary': {
                                        'Groupings': [
                                            {
                                                'Projections': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
                                                'Subtotal': 1,
                                            },
                                        ],
                                    },
                                    'DataReduction': {
                                        'DataVolume': 3,
                                        'Primary': {
                                            'Window': {
                                                'Count': 50000
                                            },
                                        },
                                    },
                                    'Version': 1,
                                },
                                'ExecutionMetricsKind': 1,
                            },
                        },
                    ]
                },
                'CacheKey': '',
                'QueryId': '',
                'ApplicationContext': {
                    'DatasetId': '5dd23708-9095-4e35-b585-d1039d481990',
                    'Sources': [
                        {'ReportId': '0f6fa041-4098-458c-a4ac-1603e4eebbd2', 'VisualId': '7b566eb945004bec1197'}
                    ]
                }
            }
        ],
        'cancelQueries': [],
        'modelId': 3418545
    }

    response = requests.post(
        'https://wabi-brazil-south-api.analysis.windows.net/public/reports/querydata',
        params=params,
        headers=headers,
        json=json_data
    )

    data = response.json()
    
    # Save debug response for troubleshooting
    with open("debug_response.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    if "error" in data:
        raise Exception(f"API Error: {data['error']}")

    print("   API request successful, response saved to debug_response.json")
    return data


# ===============================================================
# BINARY MASK LOGIC - REPLACING OLD JSON PROCESSING
# ===============================================================

def reverse_binary_16bit(value):
    """Convert integer to 16-bit binary and reverse it (LSB becomes position 0)"""
    if value is None:
        return '0000000000000000'
    binary_str = format(value, '016b')
    return binary_str[::-1]  # Reverse so LSB is at position 0

def convert_timestamp(timestamp):
    """Convert Unix timestamp (milliseconds) to YYYY-MM-DD format"""
    try:
        if timestamp:
            # Convert to number if it's a string
            if isinstance(timestamp, str):
                timestamp = float(timestamp)
            
            if timestamp > 1000000000000:  # Check if it's in milliseconds
                timestamp_seconds = timestamp / 1000
                dt = datetime.fromtimestamp(timestamp_seconds)
                return dt.strftime('%Y-%m-%d')
            elif timestamp > 1000000000:  # Check if it's in seconds
                dt = datetime.fromtimestamp(timestamp)
                return dt.strftime('%Y-%m-%d')
    except:
        pass
    return timestamp

def extract_data_from_json(json_data: Dict[str, Any]):
    """
    Extract data rows with C, R, Ø values and dictionaries using binary mask logic.
    
    Args:
        json_data: Full JSON data structure
        
    Returns:
        tuple: (data_rows, dictionaries, timestamp)
    """
    results = json_data.get('results', [])
    if not results:
        raise ValueError("No results found in JSON data")
    
    result_data = results[0].get('result', {}).get('data', {})
    timestamp = result_data.get('timestamp', '')
    
    dsr = result_data.get('dsr', {})
    ds_list = dsr.get('DS', [])
    if not ds_list:
        raise ValueError("No DS data found in JSON structure - PowerBI session may not be established")
    
    # Extract dictionaries for Type 1 columns (G1-G10)
    value_dicts = ds_list[0].get('ValueDicts', {}) if ds_list else {}
    dictionaries = {
        'G1': value_dicts.get('D0', []),
        'G2': value_dicts.get('D1', []), 
        'G3': value_dicts.get('D2', []),
        'G4': value_dicts.get('D3', []),
        'G5': value_dicts.get('D4', []),
        'G6': value_dicts.get('D5', []),
        'G7': value_dicts.get('D6', []),
        'G8': value_dicts.get('D7', []),
        'G9': value_dicts.get('D8', []),
        'G10': value_dicts.get('D9', [])
    }
    
    # Find all data rows with C arrays
    data_rows = []
    def find_rows(obj):
        if isinstance(obj, dict):
            if 'C' in obj and isinstance(obj['C'], list):
                data_rows.append(obj)
            for v in obj.values():
                find_rows(v)
        elif isinstance(obj, list):
            for item in obj:
                find_rows(item)
    
    find_rows(json_data)
    
    # Skip summary row and keep only rows with R or Ø values
    filtered_rows = []
    for row in data_rows:
        if row.get('Ø') is not None or row.get('R') is not None:
            filtered_rows.append(row)
    
    return filtered_rows, dictionaries, timestamp

def process_row_with_binary_masks(row_data, previous_row, dictionaries):
    """Process a single row using binary mask logic"""
    # Initialize result with None values
    result = [None] * 15
    columns = ['G0', 'G1', 'G2', 'G3', 'G4', 'G5', 'G6', 'G7', 'G8', 'G9', 'G10', 'M0', 'M1', 'M2', 'M3']
    
    # Get values
    c_values = row_data.get('C', [])
    r_value = row_data.get('R', 0)  # Default R = 0 if not present
    sigma_value = row_data.get('Ø', 0)  # Default Ø = 0 if not present
    
    # Convert to binary and reverse
    r_binary = reverse_binary_16bit(r_value)
    sigma_binary = reverse_binary_16bit(sigma_value)
    
    # Track value index for sequential C array assignment
    value_index = 0
    
    # Process each column (bits 0-14)
    for col_idx in range(15):
        column_name = columns[col_idx]
        
        # Get bits for this column
        r_bit = int(r_binary[col_idx])
        sigma_bit = int(sigma_binary[col_idx])
        
        # Apply logic: R inheritance has priority
        if r_bit == 1:
            # R=1: Inherit from previous row (even if NULL)
            if previous_row:
                result[col_idx] = previous_row[col_idx]
            else:
                result[col_idx] = None
        else:
            # R=0: Use Ø logic
            if sigma_bit == 1:
                # Ø=1: NULL
                result[col_idx] = None
            else:
                # Ø=0: Assign from C array
                if value_index < len(c_values):
                    raw_value = c_values[value_index]
                    
                    # Special handling for G0 (timestamp)
                    if column_name == 'G0':
                        result[col_idx] = convert_timestamp(raw_value)
                    else:
                        # Apply dictionary mapping for G columns (Type 1)
                        if column_name in dictionaries:
                            try:
                                # Convert to integer index
                                if isinstance(raw_value, str) and raw_value.isdigit():
                                    index = int(raw_value)
                                elif isinstance(raw_value, (int, float)):
                                    index = int(raw_value)
                                else:
                                    result[col_idx] = raw_value
                                    continue
                                
                                # Map using dictionary
                                dict_values = dictionaries[column_name]
                                if 0 <= index < len(dict_values):
                                    result[col_idx] = dict_values[index]
                                else:
                                    result[col_idx] = f"Index_{index}_OutOfRange"
                            except Exception:
                                result[col_idx] = raw_value
                        else:
                            result[col_idx] = raw_value
                    
                    value_index += 1
                else:
                    result[col_idx] = None
    
    return result

def convert_processed_row_to_csv(processed_row, timestamp):
    """Convert processed binary mask row to CSV format"""
    # Map processed row to CSV fields
    csv_row = {
        'scrape_datetime': timestamp,
        'date': processed_row[0] if processed_row[0] else '',  # G0
        'installation': processed_row[1] if processed_row[1] else '',  # G1
        'oil_production': processed_row[11] if processed_row[11] is not None else '',  # M0
        'gas_production': processed_row[12] if processed_row[12] is not None else '',  # M1
        'oil_equivalents_boe': processed_row[14] if processed_row[14] is not None else '',  # M3
        'operator': processed_row[2] if processed_row[2] else '',  # G2
        'type': processed_row[3] if processed_row[3] else '',  # G3
        'estado': processed_row[4] if processed_row[4] else '',  # G4
        'city': processed_row[5] if processed_row[5] else '',  # G5
        'atendimento_campo': processed_row[6] if processed_row[6] else '',  # G6
        'campo': processed_row[7] if processed_row[7] else '',  # G7
        'quantity': processed_row[13] if processed_row[13] is not None else '',  # M2
        'code': processed_row[9] if processed_row[9] is not None else '',  # G9
        'status': processed_row[10] if processed_row[10] is not None else ''  # G10
    }
    
    # Only return rows with a valid date
    if csv_row['date']:
        return csv_row
    return None

def process_data_with_binary_masks(data_rows, dictionaries, timestamp):
    """Process all data rows using binary mask logic"""
    processed_rows = []
    csv_data = []
    
    for i, row_data in enumerate(data_rows):
        # Get previous row for inheritance
        previous_row = processed_rows[-1] if processed_rows else None
        
        # Process the row with binary mask logic
        processed_row = process_row_with_binary_masks(row_data, previous_row, dictionaries)
        processed_rows.append(processed_row)
        
        # Convert to CSV format
        csv_row = convert_processed_row_to_csv(processed_row, timestamp)
        
        # Only add valid rows
        if csv_row is not None:
            csv_data.append(csv_row)
        
        # Progress indicator for large datasets
        if (i + 1) % 50 == 0:
            print(f"   Processed {i + 1}/{len(data_rows)} rows...")
    
    return csv_data

# ===============================================================
# UNCHANGED UTILITY FUNCTIONS
# ===============================================================

def write_csv_file(csv_data: List[Dict[str, Union[str, int, float]]], output_filename: str):
    """
    Write CSV data to file with exact column ordering.
    
    Args:
        csv_data: List of dictionaries with CSV row data
        output_filename: Output CSV filename
    """
    fieldnames = [
        'scrape_datetime', 'date', 'installation', 'oil_production', 
        'gas_production', 'oil_equivalents_boe', 'operator', 'type', 
        'estado', 'city', 'atendimento_campo', 'campo', 'quantity', 
        'code', 'status'
    ]
    
    with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for row in csv_data:
            clean_row = {}
            for field in fieldnames:
                value = row.get(field, '')
                # Convert all values to strings for CSV
                if value == '' or value is None:
                    clean_row[field] = ''
                else:
                    clean_row[field] = str(value)
            writer.writerow(clean_row)


def process_json_to_csv(json_data: Dict[str, Any], output_file: str):
    """
    Step 3: Process JSON response and convert to CSV format using binary mask logic.
    
    Args:
        json_data: The JSON response from PowerBI API
        output_file: Output CSV filename
    """
    print("Step 3: Processing JSON response with binary mask logic...")
    
    try:
        print("   Extracting data rows and dictionaries...")
        data_rows, dictionaries, timestamp = extract_data_from_json(json_data)
        print(f"   Found {len(data_rows)} data rows with C, R, and Ø values")
        
        print("   Applying binary mask logic to process rows...")
        csv_data = process_data_with_binary_masks(data_rows, dictionaries, timestamp)
        
        # Filter out None rows
        valid_csv_data = [row for row in csv_data if row is not None]
        
        print(f"   Writing {len(valid_csv_data)} rows to {output_file}...")
        write_csv_file(valid_csv_data, output_file)
        
        # Print summary statistics
        print(f"   Binary mask processing complete:")
        print(f"   - Total data rows processed: {len(data_rows)}")
        print(f"   - Valid CSV rows generated: {len(valid_csv_data)}")
        print(f"   - Dictionary mappings applied for G1-G10 columns")
        print(f"   - R inheritance and Ø null masks processed")
        
    except Exception as e:
        print(f"   Error processing JSON to CSV: {e}")
        raise


def run(output_file: str, historical: bool = False):
    """
    Main entry point function for the scraper.
    Called by __main__.py with output filename.
    
    Args:
        output_file: Path to output CSV file
        historical: If True, scrape maximum historical data; if False, scrape last 14 days
    """
    print(f"Starting Brazil Oil & Gas Production scraper with binary mask logic...")
    print(f"   Output file: {output_file}")
    print(f"   Historical mode: {historical}")
    
    try:
        # Step 1: Navigate and configure PowerBI dashboard
        start_date, end_date = navigate_powerbi_dashboard(historical)
        
        # Step 2: Make PowerBI API request
        json_data = make_powerbi_api_request(start_date, end_date)
        
        # Step 3: Process JSON with binary mask logic and create CSV
        process_json_to_csv(json_data, output_file)
        
        print(f"Scraping completed successfully!")
        print(f"   Output saved to: {output_file}")
        print(f"   Debug data saved to: debug_response.json")
        print(f"   Binary mask logic applied: R inheritance + Ø null masks + dictionary mappings")
        
    except Exception as e:
        print(f"Scraping failed: {e}")
        sys.exit(1)