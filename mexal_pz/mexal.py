#pip install .
from typing import Optional, Dict, List
import requests
import base64
import logging
from logging import Logger
from datetime import datetime

class MexalPZ:
    _BASE_URL = "https://services.passepartout.cloud/webapi/risorse"

    def __init__(self, domain: str, username: str, password: str, company: str, company_year: str, logger: Logger = None) -> None:
        self.logger = logger

        encoded_bytes = base64.b64encode(f"{username}:{password}".encode('utf-8'))
        base64_credentials = encoded_bytes.decode('utf-8')
        self._headers = {
                "Authorization": f"Passepartout {base64_credentials} Dominio={domain}",
                "Content-Type": "application/json",
                "Coordinate-Gestionale": f"Azienda={company} Anno={company_year}"
            }

    ##########Privati##########

    def _log_error(self, msg: str) -> None:
        if self.logger is not None:
            self.logger.error(msg)

    ##########Pubblici##########

    def get_all_categories(self) -> Optional[Dict[str, str]]:
        response = requests.get(self._BASE_URL + "/dati-generali/categorie-statistiche-cli-for", headers=self._headers, timeout=10)
        if response.status_code != 200:
            self._log_error(f"Error fetching categories: {response.status_code} - {response.text}")
            return None
        
        data = response.json()
        return {str(cat['id']): cat['descrizione'] for cat in data["dati"]}

    def get_all_customers_field(self) -> Optional[List[Dict[str, str]]]:
        response = requests.get(self._BASE_URL + "/clienti?info=true", headers=self._headers, timeout=10)
        if response.status_code != 200:
            self._log_error(f"Error fetching customers fields: {response.status_code} - {response.text}")
            return None
        
        data = response.json()
        return data["dati"]

    def get_all_customers(self, properties: Optional[List[str]] = None, include_predeleted: bool = False) -> Optional[List[Dict[str, str]]]:
        endpoint = self._BASE_URL + "/clienti"

        props = list(properties) if properties else []

        # Se non voglio i predeleted, chiedo anche il campo 'conto_precanc'
        add_precanc = False
        if not include_predeleted and "conto_precanc" not in props:
            props.append("conto_precanc")
            add_precanc = True

        if props:
            endpoint += f"?fields={','.join(props)}"

        response = requests.get(endpoint, headers=self._headers, timeout=10)
        if response.status_code != 200:
            self._log_error(f"Error fetching customers: {response.status_code} - {response.text}")
            return None
        
        data = response.json()
        customers = [{k: str(v) for k, v in d.items()} for d in data["dati"]]

        if not include_predeleted:
            customers = [c for c in customers if c.get("conto_precanc") == "N"]

            # Se ho aggiunto io conto_precanc solo per filtrare lo tolgo dai risultati
            if add_precanc:
                for c in customers:
                    c.pop("conto_precanc", None)

        return customers
    
    def get_all_referees(self, properties: Optional[List[str]] = None) -> Optional[List[Dict[str, str]]]:
        endpoint = f"{self._BASE_URL}/referenti/clienti/"
        if properties:
            endpoint += f"?fields={','.join(properties)}"
        response = requests.get(endpoint, headers=self._headers, timeout=10)
        if response.status_code != 200:
            self._log_error(f"Error fetching referees: {response.status_code} - {response.text}")
            return None
        
        data = response.json()
        return [{k: str(v) for k, v in d.items()} for d in data["dati"]]

    def get_customer_by_mexal_code(self, mexal_code: str, properties: Optional[List[str]] = None) -> Optional[Dict[str, str]]:
        endpoint = f"{self._BASE_URL}/clienti/{mexal_code}"
        if properties:
            endpoint += f"?fields={','.join(properties)}"

        response = requests.get(endpoint, headers=self._headers, timeout=10)
        if response.status_code != 200:
            self._log_error(f"Error fetching customer {mexal_code}: {response.status_code} - {response.text}")
            return None
        
        data = response.json()
        return {k: str(v) for k, v in data.items()}
    
    def get_warehouse_movements(self, year: str, properties: Optional[List[str]] = None, next: Optional[str] = None) -> Optional[List[Dict[str, str]]]:
        endpoint = f"{self._BASE_URL}/documenti/movimenti-magazzino"

        if properties:
            if "?" in endpoint:
                endpoint += f"&fields={','.join(properties)}"
            else:
                endpoint += f"?fields={','.join(properties)}"

        if next:
            if "?" in endpoint:
                endpoint += f"&next={next}"
            else:
                endpoint += f"?next={next}"

        #Remove the last 4 digit (original company year) and replace with the year parameter
        modified_headers = self._headers.copy()
        modified_headers["Coordinate-Gestionale"] = modified_headers["Coordinate-Gestionale"].split("Anno=")[0] + "Anno=" + year

        response = requests.get(endpoint, headers=modified_headers, timeout=10)
        if response.status_code != 200:
            self._log_error(f"Error fetching warehouse movements for year {year}: {response.status_code} - {response.text}")
            return None
        
        data = response.json()
        movements = [{k: str(v) for k, v in d.items()} for d in data["dati"]]
        if "next" in data:
            next_movements = self.get_warehouse_movements(year, properties, data["next"])
            if next_movements:
                movements.extend(next_movements)

        return movements
    
    def get_all_warehouse_movements(self, properties: Optional[List[str]] = None) -> Optional[List[Dict[str, str]]]:
        movements = []
        current_year = datetime.now().year
        for year in range(2019, current_year + 1):
            year_movements = self.get_warehouse_movements(str(year), properties)
            if year_movements:
                movements.extend(year_movements)

        return movements if movements else None
    
    def get_last_delivery_dates(self) -> Optional[Dict[str, str]]:
        movements = self.get_all_warehouse_movements(properties=["sigla", "sigla_doc_orig", "data_doc_orig", "cod_conto", "data_documento"])
        mov_dict = {}
        for m in movements:
            codice_mexal = m.get("cod_conto", "")

            delivery_date = ""
            if m["sigla"] == "FT":
                if m["sigla_doc_orig"] == "FT": #Fattura accompagnatoria
                    delivery_date = m["data_documento"]
                elif m["sigla_doc_orig"] == "BC": #Fattura da bolla
                    delivery_date = m["data_doc_orig"]
            elif m["sigla"] == "BC": #Bolla non ancora consegnata
                delivery_date = m["data_documento"]
            elif m["sigla"] == "BS": #Bolla di scarico dopo aver emesso la fattura
                    delivery_date = m["data_documento"]

            if codice_mexal == "" or delivery_date == "":
                continue

            if codice_mexal not in mov_dict:
                mov_dict[codice_mexal] = delivery_date
            else:
                mov_dict[codice_mexal] = max(delivery_date, mov_dict[codice_mexal])
        
        return mov_dict if mov_dict else None