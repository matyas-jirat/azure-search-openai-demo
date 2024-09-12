import asyncio
import base64
import csv
import os

import aiohttp


# Function to load base64 string from file
def load_base64_from_pdf_file(file_path):
    with open(file_path, "rb") as pdf_file:
        encoded_string = base64.b64encode(pdf_file.read())
    return encoded_string.decode("utf-8")


# Define the parser function
def parse_fields(fields):
    parsed_fields = []
    for key, value in fields.items():
        field_data = {
            "field_name": key,
            "content": value.get("content", None),
            "pageNumber": value.get("boundingRegions", [{}])[0].get("pageNumber", None),
            "confidence": value.get("confidence", None),
        }
        parsed_fields.append(field_data)
    return parsed_fields


# Function to analyze document and get apim-request-id from the response headers
async def analyze_document_async(base64_string, api_key, session):
    """Asynchronně analyzuje dokument pomocí Azure Document Intelligence."""
    url = "https://ai-viktorsohajekai089949226317.cognitiveservices.azure.com/documentintelligence/documentModels/Tatra_ner_v2:analyze?api-version=2024-07-31-preview"
    headers = {"Content-type": "application/json", "Ocp-apim-subscription-key": api_key}
    data = {"base64Source": base64_string}

    async with session.post(url, headers=headers, json=data) as response:
        apim_request_id = response.headers.get("apim-request-id", "Not Found")
        return apim_request_id


# Function to get analysis results based on apim_request_id
async def get_analysis_results_async(apim_request_id, api_key, session):
    """Asynchronně získává výsledky analýzy na základě apim_request_id, dokud není stav 'succeeded'."""

    url = f"https://ai-viktorsohajekai089949226317.cognitiveservices.azure.com/documentintelligence/documentModels/Tatra_ner_v2/analyzeResults/{apim_request_id}?api-version=2024-07-31-preview"
    headers = {"Ocp-apim-subscription-key": api_key}

    while True:
        async with session.get(url, headers=headers) as response:
            response_json = await response.json()
            if response_json["status"] == "succeeded":
                return response_json
            elif response_json["status"] == "failed":
                raise ValueError(
                    f"Azure Document Intelligence analysis failed: {response_json.get('error', 'Unknown error')}"
                )
            else:
                await asyncio.sleep(5)  # Počkáme 5 sekund před dalším pokusem


async def process_files(new_file_names, api_key):
    """Asynchronně zpracovává seznam souborů pomocí Azure Document Intelligence."""

    async with aiohttp.ClientSession() as session:
        # 1. Spustíme analýzu dokumentů paralelně
        analysis_tasks = [
            analyze_document_async(
                load_base64_from_pdf_file(os.path.join(data_folder_path, file_name)), api_key, session
            )
            for file_name in new_file_names
        ]
        apim_request_ids = await asyncio.gather(*analysis_tasks)

        # 2. Získáme výsledky analýz paralelně
        results_tasks = [
            get_analysis_results_async(apim_request_id, api_key, session) for apim_request_id in apim_request_ids
        ]
        results = await asyncio.gather(*results_tasks)

        # 3. Zpracujeme výsledky a uložíme je do metadat
        output = {}
        for file_name, result in zip(new_file_names, results):
            fields = result["analyzeResult"]["documents"][0]["fields"]
            output[file_name] = parse_fields(fields)
        
        with open(metadata_file_path, "a", newline="") as f:
            with open(data_folder_path + "partner_file_mapping.txt", "a") as partner_file_mapping:
                for file_name, parsed_fields in output.items():
                    field_values = {"contracting_party": "", "valid_to": "", "signed_date": "", "signatory_tatra": ""}
                    for field in parsed_fields:
                        if field["field_name"] in field_values:
                            field_values[field["field_name"]] = field["content"]
                    f.write(
                        f"\"{file_name}\",\"{field_values['contracting_party']}\",\"{field_values['valid_to']}\",\"{field_values['signed_date']}\",\"{field_values['signatory_tatra']}\"\n"
                    )
                    partner_file_mapping.write(f"\"{file_name}\",\"{field_values['contracting_party']}\"\n")


def check_and_get_new_files(metadata_file_path, data_folder_path):
    # Load existing metadata from CSV (if it exists), skipping the first 3 lines (header)
    processed_files = set()
    if os.path.exists(metadata_file_path):
        with open(metadata_file_path) as f:
            # Skip the first 3 lines (header)
            for _ in range(3):
                next(f)

            # Read the remaining lines and extract file names
            for line in f:
                reader = csv.reader([line])  # Create a csv reader for the single line
                for row in reader:  # There should be only one row
                    processed_files.add(row[0])  # Add the first column (file name)

    # Get all files in the data folder
    all_files = set(os.listdir(data_folder_path))
    all_pdf_files = {file for file in all_files if file.endswith(".pdf")}

    # Find new files to process
    new_files = all_pdf_files - processed_files

    if new_files:
        print(f"Found {len(new_files)} new files to process.")
        return list(new_files)  # Convert set to list for further processing
    else:
        print("No new files found to process.")
        return []  # Return an empty list if no new files are found


if __name__ == "__main__":
    data_folder_path = "./data/"
    metadata_file_path = "./data/documents_metadata.txt"
    api_key = os.environ.get("AZURE_AI_SERVICE_API_KEY")

    # Get new files to process, ensuring we have full file paths
    new_file_names = check_and_get_new_files(metadata_file_path, data_folder_path)
    if new_file_names == []:
        print("No new files")
    else:
        print(f"Processing files: {new_file_names}")
        # Process files asynchronously
        asyncio.run(process_files(new_file_names, api_key))
