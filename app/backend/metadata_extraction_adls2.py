import asyncio
import base64
import csv
from dataclasses import dataclass
import datetime
import os
import time
from typing import List, Optional
import aiohttp
import logging

from prepdocslib.blobmanager import BlobManager
from prepdocslib.listfilestrategy import File, ListFileStrategy
from azure.storage.blob.aio import BlobServiceClient


@dataclass
class MetadataExtraction:
    def __init__(
        self,
        list_file_strategy: ListFileStrategy,
        blob_manager: BlobManager,
        api_key: str,
        metadata_file_path: Optional[str] = None,
        logger: Optional[logging.Logger] = None
    ):
        self.list_file_strategy = list_file_strategy
        self.blob_manager = blob_manager
        self.api_key = api_key
        self.files: List[str] = []
        self.metadata = {}
        self.metadata_file_path = metadata_file_path
        self.logger = logger
        if not self.metadata_file_path:
            self.metadata_file_path = self._create_or_get_metadata_file()
        if not logger:
            self.logger = logging.getLogger("ingester")

    async def _create_or_get_metadata_file(self, metadata_file_path: Optional[str] = None) -> str:
        """Vytvoří nový soubor s metadaty v Blob Storage nebo použije existující."""
        if metadata_file_path:
            # Použití existujícího souboru
            try:
                 # Ověření existence souboru v Blob Storage
                async with BlobServiceClient(
                    account_url=self.blob_manager.endpoint, credential=self.blob_manager.credential
                ) as blob_service_client:
                    async with blob_service_client.get_blob_client(
                        container=self.blob_manager.container, blob=metadata_file_path
                    ) as blob_client:
                        if await blob_client.exists():
                            print(f"Používám existující soubor s metadaty: {metadata_file_path}")
                            return metadata_file_path
                        else:
                            print(f"Soubor s metadaty {metadata_file_path} neexistuje.")
                            return self._create_metadata_file_path()
            except Exception as e:
                print(f"Chyba při ověřování souboru s metadaty: {e}")
                # V případě chyby vytvoříme nový soubor
                return self._create_metadata_file_path()
        else:
            # Vytvoření nového souboru
            return self._create_metadata_file_path()

    def _create_metadata_file_path(self) -> str:
        """Vytvoří nový soubor s metadaty v Blob Storage."""
        print("Creating new documents_metadata.csv file")
        file_name = "documents_metadata.csv"
        blob_path = os.path.join(self.blob_manager.container, file_name)  # Cesta k souboru v Blob Storage
        return blob_path

    def _load_base64_from_pdf_file(self, file: File):
        """Načte PDF soubor a převede ho na base64 řetězec."""
        file.content.seek(0)  # Přesunutí kurzoru na začátek souboru
        file_content = file.content.read()  # Načtení obsahu souboru jako bytes
        return base64.b64encode(file_content).decode('utf-8')

    async def _analyze_document_async(self, base64_string, session, file_name: Optional[str]) -> tuple[str, List[str]]:
        """Asynchronně analyzuje dokument pomocí Azure Document Intelligence."""
        url = "https://ai-viktorsohajekai089949226317.cognitiveservices.azure.com/documentintelligence/documentModels/Tatra_ner_v2:analyze?api-version=2024-07-31-preview"
        headers = {"Content-type": "application/json", "Ocp-Apim-Subscription-Key": self.api_key}
        data = {"base64Source": f"{base64_string}"}
        max_retries = 12  # Maximální počet opakování
        apim_request_id = ""

        async with session.post(url, headers=headers, json=data) as response:
            apim_request_id = response.headers.get("apim-request-id", "Not Found")
            self.logger.info(f"File: {file_name} was sent for processing of metadata.")
        time.sleep(1)

        url = f"https://ai-viktorsohajekai089949226317.cognitiveservices.azure.com/documentintelligence/documentModels/Tatra_ner_v2/analyzeResults/{apim_request_id}?api-version=2024-07-31-preview"
        headers = {"Ocp-apim-subscription-key": self.api_key}

        for _ in range(max_retries):
            async with session.get(url, headers=headers) as response:
                response_json = await response.json()

                if "status" not in response_json:
                    await asyncio.sleep(5)
                    continue

                if response_json["status"] == "succeeded":
                    self.logger(f"File: {file_name} was successfully extracted.")
                    return (file_name, self._parse_fields(response_json))
                elif response_json["status"] == "failed":
                    raise ValueError(
                        f"Azure Document Intelligence analysis failed: {response_json.get('error', 'Unknown error')}"
                    )
                else:
                    await asyncio.sleep(5)


    def _parse_fields(self, result):
        """Zpracuje pole extrahovaná z Azure Document Intelligence."""
        fields = result["analyzeResult"]["documents"][0]["fields"]
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

    async def run_extraction(self):
        """Spustí extrakci metadat z nových souborů."""
        async with aiohttp.ClientSession() as session:
            tasks = []
            async for file in self.list_file_strategy.list():
                if not file:
                    self.logger.info("Nebyly nalezeny žádné nové soubory ke zpracování.")
                    return
                if file.filename() == self.metadata_file_path:
                    continue
                tasks.append(self._analyze_document_async(self._load_base64_from_pdf_file(file), session, file_name=file.filename()))

            result_collection = {}
            for future in asyncio.as_completed(tasks):  # Postupné zpracování tasků
                try:
                    result = await future
                    if result is not None:
                        file_name, results = result
                        result_collection[file_name] = results
                except Exception as e:
                    self.logger.error(f"Chyba při analýze dokumentu: {e}")

            await session.close()
            self.metadata = result_collection

    async def reupload_metadata(self):
        """Uloží metadata do souboru a znovu nahraje do úložiště."""
        if not self.metadata:
            self.logger.info("No metadata")
            return

        try:
            # Uložení metadat do CSV souboru
            with open(f"{os.path.basename(self.metadata_file_path)}.csv", "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(
                    ["file_name", "contracting_party", "valid_to", "signed_date", "signatory_tatra"]
                )  # Záhlaví
                for file_name, parsed_fields in self.metadata.items():
                    field_values = {"contracting_party": "", "valid_to": "", "signed_date": "", "signatory_tatra": ""}
                    for field in parsed_fields:
                        if field["field_name"] in field_values:
                            field_values[field["field_name"]] = field["content"]
                    writer.writerow(
                        [
                            file_name,
                            field_values["contracting_party"],
                            field_values["valid_to"],
                            field_values["signed_date"],
                            field_values["signatory_tatra"],
                        ]
                    )
            delimiter=","
            with open(f"{os.path.basename(self.metadata_file_path)}.csv", "r", newline="", encoding="utf-8") as csv_f:
                with open(self.metadata_file_path, "w", newline="", encoding="utf-8") as txt_f:
                    reader = csv.reader(csv_f, delimiter=delimiter)
                    txt_f.write("Use this file for all overview questions as you can easily count amount of partners, for getting information about sign dates, penalties, who signed the documents and contracting parties/partners. Example questions: List all partners in bullet points? How many partners we have in database? \n \n")
                    for row in reader:
                        txt_f.write(delimiter.join(row) + '\n')

            # Vytvoření objektu File pro BlobManager
            file_to_upload = File(
                content=open(self.metadata_file_path, "rb"),  # Otevření souboru v binárním režimu pro čtení
                url=None,
            )

            # Nahrání souboru pomocí BlobManageru
            await self.blob_manager.upload_blob(file_to_upload)
            self.logger.info(f"Metadata file {self.metadata_file_path} was uploaded to {self.blob_manager.account}: {self.blob_manager.container}.")

        except Exception as e:
            self.logger.error(f"Chyba při ukládání nebo nahrávání metadat: {e}")

    async def run(self):
        """Spustí extrakci metadat a jejich nahrání do Blob Storage."""
        self.logger.info(f"Extracting metadata from files within {self.blob_manager.container} in {self.blob_manager.account}.")
        try:
            await self.run_extraction()
            self.logger.info("Metadata extraction run successfully.")
            await self.reupload_metadata()

        except Exception as e:
            self.logger.error(f"Chyba během extrakce a nahrávání metadat: {e}.")
