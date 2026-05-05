#pip install .
from typing import Any, Optional
import requests
import base64
from logging import Logger
from datetime import datetime
import re

class MexalPZ:
    _BASE_URL = "https://services.passepartout.cloud/webapi/risorse"
    _TIMEOUT_SECONDS = 20

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

    def _get_mydb(self, app_name: str, mydb_name: str, id: Optional[str] = None) -> Optional[Any]:
        base_endpoint = f"{self._BASE_URL}/mydb/{app_name}@{mydb_name}"

        if id:
            url = f"{base_endpoint}/{id}"
            response = requests.get(url, headers=self._headers, timeout=self._TIMEOUT_SECONDS)
            return response.json() if response.status_code == 200 else None

        all_records = []
        next_token = None

        while True:
            params = {}
            if next_token:
                params['next'] = next_token

            response = requests.get(
                base_endpoint, 
                headers=self._headers, 
                params=params, 
                timeout=self._TIMEOUT_SECONDS
            )

            if response.status_code != 200:
                self._log_error(f"Error fetching mydb: {response.status_code} - {response.text}")
                return None

            data = response.json()
            all_records.extend(data.get('dati', []))

            next_token = data.get('next')
            if not next_token:
                break

        return all_records
    
    def _find_mydb(self, app_name: str, mydb_name: str, filters: list[tuple[str, str, Any]] = []) -> Optional[Any]:
        endpoint = self._BASE_URL + f"/mydb/{app_name}@{mydb_name}/ricerca"

        filters = {
            "filtri": [
                {
                    "campo": campo,
                    "condizione": condizione,
                    "valore": valore
                } for campo, condizione, valore in filters
            ]
        }

        response = requests.post(endpoint, headers=self._headers, json=filters, timeout=self._TIMEOUT_SECONDS)
        if response.status_code != 200:
            self._log_error(f"Error searching mydb records: {response.status_code} - {response.text}")
            return None

        data = response.json()
        return data

    ##########Pubblici##########

    def get_all_categories(self) -> Optional[dict[str, str]]:
        response = requests.get(self._BASE_URL + "/dati-generali/categorie-statistiche-cli-for", headers=self._headers, timeout=self._TIMEOUT_SECONDS)
        if response.status_code != 200:
            self._log_error(f"Error fetching categories: {response.status_code} - {response.text}")
            return None
        
        data = response.json()
        return {str(cat['id']): cat['descrizione'] for cat in data["dati"]}

    def get_all_aspetti_esteriori_beni(self) -> Optional[dict[str, str]]:
        response = requests.get(self._BASE_URL + "/dati-generali/aspetto-esteriore-beni", headers=self._headers, timeout=self._TIMEOUT_SECONDS)
        if response.status_code != 200:
            self._log_error(f"Error fetching exterioraspects: {response.status_code} - {response.text}")
            return None
        
        data = response.json()
        return {str(cat['codice']): cat['descrizione'] for cat in data["dati"]}

    def get_all_customers_field(self) -> Optional[list[dict[str, str]]]:
        response = requests.get(self._BASE_URL + "/clienti?info=true", headers=self._headers, timeout=self._TIMEOUT_SECONDS)
        if response.status_code != 200:
            self._log_error(f"Error fetching customers fields: {response.status_code} - {response.text}")
            return None
        
        data = response.json()
        return data["dati"]

    def get_all_warehouse_movements_field(self) -> Optional[list[dict[str, str]]]:
        response = requests.get(self._BASE_URL + "/documenti/movimenti-magazzino?info=true", headers=self._headers, timeout=self._TIMEOUT_SECONDS)
        if response.status_code != 200:
            self._log_error(f"Error fetching warehouse movements fields: {response.status_code} - {response.text}")
            return None
        
        data = response.json()
        return data["dati"]

    def get_all_customers(self, properties: Optional[list[str]] = None, include_predeleted: bool = False) -> Optional[list[dict[str, str]]]:
        endpoint = self._BASE_URL + "/clienti"

        props = list(properties) if properties else []

        # Se non voglio i predeleted, chiedo anche il campo 'conto_precanc'
        add_precanc = False
        if not include_predeleted and "conto_precanc" not in props:
            props.append("conto_precanc")
            add_precanc = True

        if props:
            endpoint += f"?fields={','.join(props)}"

        response = requests.get(endpoint, headers=self._headers, timeout=self._TIMEOUT_SECONDS)
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
    
    def get_all_referees(self, properties: Optional[list[str]] = None) -> Optional[list[dict[str, str]]]:
        endpoint = f"{self._BASE_URL}/referenti/clienti/"
        if properties:
            endpoint += f"?fields={','.join(properties)}"
        response = requests.get(endpoint, headers=self._headers, timeout=self._TIMEOUT_SECONDS)
        if response.status_code != 200:
            self._log_error(f"Error fetching referees: {response.status_code} - {response.text}")
            return None
        
        data = response.json()
        return [{k: str(v) for k, v in d.items()} for d in data["dati"]]

    def get_customer_by_mexal_code(self, mexal_code: str, properties: Optional[list[str]] = None) -> Optional[dict[str, str]]:
        endpoint = f"{self._BASE_URL}/clienti/{mexal_code}"
        if properties:
            endpoint += f"?fields={','.join(properties)}"

        response = requests.get(endpoint, headers=self._headers, timeout=self._TIMEOUT_SECONDS)
        if response.status_code != 200:
            self._log_error(f"Error fetching customer {mexal_code}: {response.status_code} - {response.text}")
            return None

        data = response.json()
        return {k: str(v) for k, v in data.items()}

    def get_warehouse_movements(self, year: str, properties: Optional[list[str]] = None) -> Optional[list[dict[str, str]]]:
        base_endpoint = f"{self._BASE_URL}/documenti/movimenti-magazzino"

        modified_headers = self._headers.copy()
        header_coord = modified_headers.get("Coordinate-Gestionale", "")
        modified_headers["Coordinate-Gestionale"] = re.sub(r"Anno=\d{4}", f"Anno={year}", header_coord)

        all_movements = []
        next = None

        while True:
            params = {}
            if properties:
                params["fields"] = ",".join(properties)
            if next:
                params["next"] = next

            response = requests.get(
                base_endpoint, 
                headers=modified_headers, 
                params=params, 
                timeout=self._TIMEOUT_SECONDS
            )
            
            if response.status_code != 200:
                self._log_error(f"Error fetching warehouse movements for year {year}: {response.status_code} - {response.text}")
                return None
            
            data = response.json()

            movements = [{k: str(v) for k, v in d.items()} for d in data.get("dati", [])]
            all_movements.extend(movements)

            next = data.get("next")
            if not next:
                break

        return all_movements
    
    def find_warehouse_movements(self, year: str, properties: list[str] = [], filters: list[tuple[str, str, Any]] = []) -> Optional[list[dict[str, str]]]:
        base_endpoint = f"{self._BASE_URL}/documenti/movimenti-magazzino/ricerca"

        modified_headers = self._headers.copy()
        header_coord = modified_headers.get("Coordinate-Gestionale", "")
        modified_headers["Coordinate-Gestionale"] = re.sub(r"Anno=\d{4}", f"Anno={year}", header_coord)

        all_movements = []
        next = None

        filters = {
            "filtri": [
                {
                    "campo": campo,
                    "condizione": condizione,
                    "valore": valore
                } for campo, condizione, valore in filters
            ]
        }

        while True:
            params = {}
            if properties:
                params["fields"] = ",".join(properties)
            if next:
                params["next"] = next

            response = requests.post(
                base_endpoint, 
                headers=modified_headers, 
                params=params,
                json=filters,
                timeout=self._TIMEOUT_SECONDS
            )
            
            if response.status_code != 200:
                self._log_error(f"Error fetching warehouse movements for year {year}: {response.status_code} - {response.text}")
                return None
            
            data = response.json()

            movements = [{k: str(v) for k, v in d.items()} for d in data.get("dati", [])]
            all_movements.extend(movements)

            next = data.get("next")
            if not next:
                break

        return all_movements

    def get_all_warehouse_movements(self, properties: Optional[list[str]] = None) -> Optional[list[dict[str, str]]]:
        movements = []
        current_year = datetime.now().year
        for year in range(2019, current_year + 1):
            year_movements = self.get_warehouse_movements(str(year), properties)
            if year_movements:
                movements.extend(year_movements)

        return movements if movements else None
    
    def get_last_delivery_dates(self) -> Optional[dict[str, str]]:
        movements = self.get_all_warehouse_movements(
            properties=["sigla", "sigla_doc_orig", "data_doc_orig", "cod_conto", "data_documento"]
        )
        mov_dict = {}
        for m in movements:
            codice_mexal = m.get("cod_conto")
            if not codice_mexal:
                continue

            sigla = m.get("sigla", "")
            sigla_doc_orig = m.get("sigla_doc_orig", "")

            delivery_date = None
            if sigla == "FT":
                if sigla_doc_orig == "FT": #Fattura accompagnatoria
                    delivery_date = m["data_documento"]
                elif sigla_doc_orig == "BC": #Fattura da bolla
                    delivery_date = m["data_doc_orig"]
            elif sigla == "BC": #Bolla non ancora consegnata
                delivery_date = m["data_documento"]
            elif sigla == "BS": #Bolla di scarico dopo aver emesso la fattura
                delivery_date = m["data_documento"]

            if not delivery_date:
                continue

            current_max = mov_dict.get(codice_mexal)
            if not current_max or delivery_date > current_max:
                mov_dict[codice_mexal] = delivery_date
        
        return mov_dict if mov_dict else None

    def get_indirizzo_di_spedizione(
            self,
            code: str,
            properties: Optional[list[str]] = ["cod_conto", "descrizione", "indirizzo", "cap", "localita", "provincia", "tp_nazionalita", "cod_paese"]
        ) -> Optional[dict[str, str]]:
        endpoint = f"{self._BASE_URL}/indirizzi-spedizione/{code}"

        params = {"fields": ",".join(properties)} if properties else {}

        try:
            response = requests.get(
                endpoint,
                headers=self._headers,
                params=params,
                timeout=self._TIMEOUT_SECONDS
            )
            response.raise_for_status()
            data = response.json()
            return {k: str(v) for k, v in data.items()}

        except requests.exceptions.RequestException as e:
            self._log_error(f"Network error fetching shipping address {code}: {str(e)}")
        except Exception as e:
            self._log_error(f"Validation error for address {code}: {str(e)}")

        return None

    # Mydb

    def get_note_indirizzi_spedizione(self, id: Optional[str] = None) -> Optional[dict[str, str]]:
        '''
        Recupera le note sugli indirizzi di spedizione dalla mydb "noteind". Se viene passato un id, recupera solo la nota con quell'id, altrimenti recupera tutte le note.
        '''
        return self._get_mydb("430569NOTE", "noteind", id)
    
    def get_note_consegna(self, id: Optional[str] = None) -> Optional[dict[str, str]]:
        '''
        Recupera le note sulla consegna dalla mydb "notecons". Se viene passato un id, recupera solo la nota con quell'id, altrimenti recupera tutte le note.
        '''
        return self._get_mydb("430569PERSONAL", "notecons", id)
    
    def find_note_indirizzi_spedizione(self, filters: list[tuple[str, str, str]] = []) -> Optional[Any]:
        return self._find_mydb("430569NOTE", "noteind", filters)

    def find_note_consegna(self, filters: list[tuple[str, str, str]] = []) -> Optional[Any]:
        return self._find_mydb("430569PERSONAL", "notecons", filters)