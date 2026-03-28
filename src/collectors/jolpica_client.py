"""
Cliente para a Jolpica F1 API.
Base URL: https://api.jolpi.ca/ergast/f1/

Responsabilidades:
  - Fazer requisições com retry automático
  - Respeitar o rate limit da API (500 req/hora, burst de 4)
  - Paginar automaticamente para buscar todos os registros
  - Salvar os JSONs brutos em data/raw/jolpica/
"""

import json
import time
from pathlib import Path
from typing import Any

import requests
from loguru import logger
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ─── Constantes ───────────────────────────────────────────────────────────────

BASE_URL = "https://api.jolpi.ca/ergast/f1"
RAW_DIR  = Path("data/raw/jolpica")

# A API limita 500 req/hora
RATE_LIMIT_DELAY = 0.25
PAGE_SIZE        = 100


# ─── Cliente ──────────────────────────────────────────────────────────────────

class JolpicaClient:
    """
    Wrapper para a Jolpica F1 API.

    Uso básico:
        client = JolpicaClient()
        drivers = client.get_drivers(season=2024)
        results = client.get_race_results(season=2024, round=1)
    """

    def __init__(self, save_raw: bool = True):
        self.save_raw = save_raw
        self.session  = self._build_session()

        if save_raw:
            RAW_DIR.mkdir(parents=True, exist_ok=True)

    # ── Setup interno ─────────────────────────────────────────────────────────

    def _build_session(self) -> requests.Session:
        session = requests.Session()

        retry_strategy = Retry(
            total=3,                              # tenta 3 vezes
            backoff_factor=2,                     # espera 2s, 4s, 8s entre tentativas
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.headers.update({"Accept": "application/json"})
        return session

    # ── Requisição base ───────────────────────────────────────────────────────

    def _get(self, endpoint: str, params: dict | None = None) -> dict:
        """
        Faz uma requisição GET e retorna o MRData.
        """
        url = f"{BASE_URL}{endpoint}.json"
        logger.debug(f"GET {url} | params={params}")

        time.sleep(RATE_LIMIT_DELAY)

        response = self.session.get(url, params=params, timeout=15)
        response.raise_for_status()

        data = response.json()
        return data["MRData"]

    # ── Paginação automática ──────────────────────────────────────────────────

    def _get_all(self, endpoint: str, data_key: str) -> list[dict]:
        """
        Busca todos os registros de um endpoint paginado.

        A API retorna no máximo PAGE_SIZE registros por vez. Este método
        faz quantas requisições forem necessárias e devolve tudo junto.
        """
        offset = 0
        all_items: list[dict] = []

        while True:
            mr_data = self._get(endpoint, params={"limit": PAGE_SIZE, "offset": offset})

            total  = int(mr_data["total"])
            table  = mr_data[data_key]

            # Cada tabela tem uma lista com nome diferente: Races, Drivers, etc.
            # Pega o primeiro valor que for uma lista dentro da tabela
            items = next(
                (v for v in table.values() if isinstance(v, list)),
                []
            )
            all_items.extend(items)

            logger.info(
                f"{endpoint} | offset={offset} | "
                f"recebidos={len(items)} | total={total}"
            )

            offset += PAGE_SIZE
            if offset >= total:
                break

        if self.save_raw:
            self._save(endpoint, all_items)

        return all_items

    # ── Persistência local ────────────────────────────────────────────────────

    def _save(self, endpoint: str, data: Any) -> None:
        """Salva o JSON bruto em data/raw/jolpica/ para reuso futuro."""
        filename = endpoint.strip("/").replace("/", "_") + ".json"
        filepath = RAW_DIR / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        logger.success(f"Salvo em {filepath}")

    # ─── Endpoints públicos ───────────────────────────────────────────────────
    # Cada método abaixo corresponde a um endpoint da Jolpica API.

    def get_seasons(self) -> list[dict]:
        """
        Retorna todas as temporadas disponíveis na API.
        """
        logger.info("Buscando temporadas...")
        return self._get_all("/seasons", "SeasonTable")

    def get_drivers(self, season: int | str | None = None) -> list[dict]:
        """
        Retorna os pilotos. Se season for informado, filtra pela temporada.
        """
        endpoint = f"/{season}/drivers" if season else "/drivers"
        logger.info(f"Buscando pilotos | season={season or 'todos'}")
        return self._get_all(endpoint, "DriverTable")

    def get_constructors(self, season: int | str | None = None) -> list[dict]:
        """
        Retorna os construtores (equipes).
        """
        endpoint = f"/{season}/constructors" if season else "/constructors"
        logger.info(f"Buscando construtores | season={season or 'todos'}")
        return self._get_all(endpoint, "ConstructorTable")

    def get_circuits(self, season: int | str | None = None) -> list[dict]:
        """
        Retorna os circuitos. Inclui coordenadas geográficas (lat/long).
        """
        endpoint = f"/{season}/circuits" if season else "/circuits"
        logger.info(f"Buscando circuitos | season={season or 'todos'}")
        return self._get_all(endpoint, "CircuitTable")

    def get_races(self, season: int | str) -> list[dict]:
        """
        Retorna o calendário de corridas de uma temporada.
        """
        logger.info(f"Buscando calendário | season={season}")
        return self._get_all(f"/{season}/races", "RaceTable")

    def get_race_results(self, season: int | str, round: int | str | None = None,) -> list[dict]:
        """
        Retorna resultados de corrida.
        """
        endpoint = (
            f"/{season}/{round}/results" if round
            else f"/{season}/results"
        )
        logger.info(f"Buscando resultados | season={season} | round={round or 'todos'}")
        return self._get_all(endpoint, "RaceTable")

    def get_qualifying(self, season: int | str, round: int | str | None = None,) -> list[dict]:
        """
        Retorna resultados de classificação (Q1, Q2, Q3).
        """
        endpoint = (
            f"/{season}/{round}/qualifying" if round
            else f"/{season}/qualifying"
        )
        logger.info(f"Buscando qualifying | season={season} | round={round or 'todos'}")
        return self._get_all(endpoint, "RaceTable")

    def get_lap_times(self, season: int | str, round: int | str, lap: int | None = None,) -> list[dict]:
        """
        Retorna tempos de volta de uma corrida.

        Dados disponíveis apenas de 1996 em diante.
        """
        endpoint = (
            f"/{season}/{round}/laps/{lap}" if lap
            else f"/{season}/{round}/laps"
        )
        logger.info(
            f"Buscando tempos de volta | season={season} | "
            f"round={round} | lap={lap or 'todas'}"
        )
        return self._get_all(endpoint, "RaceTable")

    def get_pit_stops(self, season: int | str, round: int | str,) -> list[dict]:
        """
        Retorna dados de pit stops de uma corrida.

        Dados disponíveis apenas de 2012 em diante.
        """
        logger.info(f"Buscando pit stops | season={season} | round={round}")
        return self._get_all(f"/{season}/{round}/pitstops", "RaceTable")

    def get_driver_standings(self, season: int | str, round: int | str | None = None,) -> list[dict]:
        """
        Retorna o campeonato de pilotos.
        """
        endpoint = (
            f"/{season}/{round}/driverstandings" if round
            else f"/{season}/driverstandings"
        )
        logger.info(f"Buscando campeonato pilotos | season={season}")
        return self._get_all(endpoint, "StandingsTable")

    def get_constructor_standings(self, season: int | str, round: int | str | None = None,) -> list[dict]:
        """
        Retorna o campeonato de construtores.
        """
        endpoint = (
            f"/{season}/{round}/constructorstandings" if round
            else f"/{season}/constructorstandings"
        )
        logger.info(f"Buscando campeonato construtores | season={season}")
        return self._get_all(endpoint, "StandingsTable")

    def get_sprint_results(self, season: int | str, round: int | str | None = None,) -> list[dict]:
        """
        Retorna resultados das corridas sprint.
        """
        endpoint = (
            f"/{season}/{round}/sprint" if round
            else f"/{season}/sprint"
        )
        logger.info(f"Buscando sprint results | season={season}")
        return self._get_all(endpoint, "RaceTable")


# ─── Exemplo de uso ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Configura o logger para mostrar apenas INFO
    logger.remove()
    logger.add(
        sink=lambda msg: print(msg, end=""),
        level="INFO",
        format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}",
    )

    client = JolpicaClient(save_raw=True)

    # Busca o calendário e os resultados do GP 1 de 2024
    races   = client.get_races(season=2024)
    results = client.get_race_results(season=2024, round=1)
    pits    = client.get_pit_stops(season=2024, round=1)

    print(f"\nCorridas em 2024:  {len(races)}")
    print(f"Pilotos no GP 1:   {len(results[0]['Results'])}")
    print(f"Pit stops no GP 1: {len(pits[0]['PitStops'])}")