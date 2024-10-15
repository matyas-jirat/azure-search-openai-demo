import asyncio
import base64
import csv
from dataclasses import dataclass
import os
from typing import List, Optional
import aiohttp

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
    ):
        self.list_file_strategy = list_file_strategy
        self.blob_manager = blob_manager
        self.api_key = api_key
        self.files: List[str] = []
        self.metadata = {}
        self.metadata_file_path = metadata_file_path
        if not self.metadata_file_path:
            self.metadata_file_path = self._create_or_get_metadata_file()

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
                            return self._create_metadata_file_in_blob_storage()
            except Exception as e:
                print(f"Chyba při ověřování souboru s metadaty: {e}")
                # V případě chyby vytvoříme nový soubor
                return self._create_metadata_file_in_blob_storage()
        else:
            # Vytvoření nového souboru
            return self._create_metadata_file_in_blob_storage()

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
        encoded_string = base64.b64encode(file_content)
        return encoded_string.decode("utf-8")

    async def _analyze_document_async(self, base64_string, session):
        """Asynchronně analyzuje dokument pomocí Azure Document Intelligence."""
        url = "https://ai-viktorsohajekai089949226317.cognitiveservices.azure.com/documentintelligence/documentModels/Tatra_ner_v2:analyze?api-version=2024-07-31-preview"
        headers = {"Content-type": "application/json", "Ocp-apim-subscription-key": self.api_key}
        data = {"base64Source": base64_string}

        async with session.post(url, headers=headers, json=data) as response:
            apim_request_id = response.headers.get("apim-request-id", "Not Found")
            return apim_request_id

    async def _get_analysis_results_async(self, apim_request_id, session):
        """Asynchronně získává výsledky analýzy na základě apim_request_id, dokud není stav 'succeeded'."""
        url = f"https://ai-viktorsohajekai089949226317.cognitiveservices.azure.com/documentintelligence/documentModels/Tatra_ner_v2/analyzeResults/{apim_request_id}?api-version=2024-07-31-preview"
        headers = {"Ocp-apim-subscription-key": self.api_key}

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

    def _parse_fields(self, fields):
        """Zpracuje pole extrahovaná z Azure Document Intelligence."""
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
        async for file in self.list_file_strategy.list():  # Použití async for
            if not file:
                print("Nebyly nalezeny žádné nové soubory ke zpracování.")
                return

            async with aiohttp.ClientSession() as session:
                # 1. Spustíme analýzu dokumentu
                apim_request_id = await self._analyze_document_async(
                    self._load_base64_from_pdf_file(file), session
                )

                # 2. Získáme výsledky analýzy
                result = await self._get_analysis_results_async(apim_request_id, session)

                # 3. Zpracujeme výsledky a uložíme je do metadat
                fields = result["analyzeResult"]["documents"][0]["fields"]
                self.metadata[file.filename()] = self._parse_fields(fields)
        
        await session.close()

    async def reupload_metadata(self):
        """Uloží metadata do souboru a znovu nahraje do úložiště."""
        if not self.metadata:
            print("No metadata")
            return

        try:
            # Uložení metadat do CSV souboru
            with open(self.metadata_file_path, "w", newline="", encoding="utf-8") as f:
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
            print("created csv file")

            # Vytvoření objektu File pro BlobManager
            file_to_upload = File(
                content=open(self.metadata_file_path, "rb"),  # Otevření souboru v binárním režimu pro čtení
                url=None,
            )
            print(file_to_upload.url)
            print(file_to_upload.filename())
            print(file_to_upload.content)

            # Nahrání souboru pomocí BlobManageru
            await self.blob_manager.upload_blob(file_to_upload)
            print(f"Metadata file {self.metadata_file_path} was uploaded to {self.blob_manager.account}: {self.blob_manager.container}.")

        except Exception as e:
            print(f"Chyba při ukládání nebo nahrávání metadat: {e}")

    async def run(self):
        """Spustí extrakci metadat a jejich nahrání do Blob Storage."""
        print(f"Extracting metadata from files within {self.blob_manager.container} in {self.blob_manager.account}.")
        try:
            await self.run_extraction()
            print("Metadata extraction run successfully.")
            await self.reupload_metadata()
        except Exception as e:
            print(f"Chyba během extrakce a nahrávání metadat: {e}")
