"""
src/collectors/scraper.py

Web scraper para dados que não existem nas APIs:
  1. Wikipedia  — dados estáticos de circuito (comprimento, curvas, zonas DRS)
  2. RaceFans   — manchetes e notas de corrida por GP

Boas práticas implementadas:
  - User-Agent realista para não ser bloqueado
  - Delay entre requests para não sobrecarregar os servidores
  - Salva HTMLs brutos em data/raw/scraped/ antes de parsear
  - Try/except granular: falha em um circuito não derruba o resto
"""

import json
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from loguru import logger

# ─── Constantes ───────────────────────────────────────────────────────────────

RAW_DIR     = Path("data/raw/scraped")
DELAY       = 1.5
TIMEOUT     = 15

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Mapeamento circuitId (Jolpica) → slug da Wikipedia
# Necessário porque os IDs da Jolpica não batem com os títulos da Wikipedia
CIRCUIT_WIKIPEDIA = {
    "bahrain":          "Bahrain_International_Circuit",
    "jeddah":           "Jeddah_Street_Circuit",
    "albert_park":      "Albert_Park_Circuit",
    "suzuka":           "Suzuka_International_Racing_Course",
    "shanghai":         "Shanghai_International_Circuit",
    "miami":            "Miami_International_Autodrome",
    "imola":            "Autodromo_Enzo_e_Dino_Ferrari",
    "monaco":           "Circuit_de_Monaco",
    "villeneuve":       "Circuit_Gilles_Villeneuve",
    "catalunya":        "Circuit_de_Barcelona-Catalunya",
    "red_bull_ring":    "Red_Bull_Ring",
    "silverstone":      "Silverstone_Circuit",
    "hungaroring":      "Hungaroring",
    "spa":              "Circuit_de_Spa-Francorchamps",
    "zandvoort":        "Circuit_Zandvoort",
    "monza":            "Autodromo_Nazionale_Monza",
    "baku":             "Baku_City_Circuit",
    "marina_bay":       "Marina_Bay_Street_Circuit",
    "americas":         "Circuit_of_the_Americas",
    "rodriguez":        "Autodromo_Hermanos_Rodriguez",
    "interlagos":       "Autodromo_Jose_Carlos_Pace",
    "vegas":            "Las_Vegas_Street_Circuit",
    "losail":           "Losail_International_Circuit",
    "yas_marina":       "Yas_Marina_Circuit",
}

# Fallback hardcoded de zonas DRS (era 2022–2024).
# O infobox da Wikipedia é inconsistente nesse campo e já removeu
# a info de vários circuitos após o DRS ser abolido em 2026.
# Fonte: layouts oficiais FIA por temporada.
DRS_ZONES_FALLBACK = {
    "bahrain":       3,
    "jeddah":        3,
    "albert_park":   4,
    "suzuka":        1,
    "shanghai":      2,
    "miami":         3,
    "imola":         1,
    "monaco":        1,
    "villeneuve":    3,
    "catalunya":     2,
    "red_bull_ring": 3,
    "silverstone":   2,
    "hungaroring":   2,
    "spa":           2,
    "zandvoort":     2,
    "monza":         2,
    "baku":          2,
    "marina_bay":    3,
    "americas":      2,
    "rodriguez":     3,
    "interlagos":    2,
    "vegas":         2,
    "losail":        2,
    "yas_marina":    2,
}
 


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _get(url: str) -> BeautifulSoup | None:
    """Faz GET, loga e retorna BeautifulSoup. Retorna None em caso de erro."""
    try:
        time.sleep(DELAY)
        logger.debug(f"GET {url}")
        response = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        response.raise_for_status()
        return BeautifulSoup(response.text, "lxml")
    except requests.RequestException as e:
        logger.error(f"Falha ao acessar {url}: {e}")
        return None


def _save_raw(filename: str, data: dict | list) -> None:
    """Salva os dados parseados como JSON em data/raw/scraped/."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    path = RAW_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.success(f"Salvo: {path}")


def _clean_text(text: str) -> str:
    """Remove espaços extras, notas de rodapé [1] e caracteres unicode estranhos."""
    text = re.sub(r"\[.*?\]", "", text)       # remove [1], [a], [nota]
    text = re.sub(r"\s+", " ", text)          # normaliza espaços
    text = text.replace("\xa0", " ")          # non-breaking space
    return text.strip()


# ─── 1. Wikipedia — dados de circuito ────────────────────────────────────────

class WikipediaCircuitScraper:
    """
    Raspa o infobox da Wikipedia para cada circuito e extrai:
      - comprimento da pista (km)
      - número de curvas
      - número de zonas DRS
      - tipo de circuito (permanente / rua)
      - primeiro GP realizado (ano)

    Os infoboxes da Wikipedia são tabelas com classe "infobox".
    Cada linha tem um <th> com o label e um <td> com o valor.
    """

    BASE_URL = "https://en.wikipedia.org/wiki/{slug}"

    def scrape_circuit(self, circuit_id: str) -> dict | None:
        slug = CIRCUIT_WIKIPEDIA.get(circuit_id)
        if not slug:
            logger.warning(f"Slug Wikipedia não mapeado: {circuit_id}")
            return None
 
        url  = self.BASE_URL.format(slug=slug)
        soup = _get(url)
        if not soup:
            return None
 
        infobox = soup.find("table", class_=re.compile(r"infobox"))
        if not infobox:
            logger.warning(f"Infobox não encontrado: {circuit_id}")
            return None
 
        raw_fields: dict[str, str] = {}
        for row in infobox.find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td:
                label = _clean_text(th.get_text()).lower()
                value = _clean_text(td.get_text())
                raw_fields[label] = value
 
        # DRS: tenta infobox, cai no fallback se não encontrar
        drs_from_infobox = self._parse_int(
            raw_fields, ["drs zones", "drs zone", "drs"]
        )
        drs_zones = drs_from_infobox \
            if drs_from_infobox is not None \
            else DRS_ZONES_FALLBACK.get(circuit_id)
 
        if drs_from_infobox is None:
            logger.debug(f"DRS '{circuit_id}' veio do fallback: {drs_zones}")
 
        result = {
            "circuit_id":    circuit_id,
            "wikipedia_slug": slug,
            "length_km":     self._parse_length(raw_fields),
            "corners":       self._parse_int(raw_fields, ["corners", "turns"]),
            "drs_zones":     drs_zones,
            "circuit_type":  self._parse_circuit_type(raw_fields),
            "first_gp_year": self._parse_first_gp(raw_fields),
        }
 
        logger.info(
            f"Circuito raspado | {circuit_id} | "
            f"{result['length_km']} km | "
            f"{result['corners']} curvas | "
            f"{result['drs_zones']} DRS"
        )
        return result

    def scrape_all(self, circuit_ids: list[str]) -> list[dict]:
        """
        Raspa dados de uma lista de circuitos e salva em JSON.
        """
        logger.info(f"Raspando {len(circuit_ids)} circuitos da Wikipedia...")
        results = []

        for circuit_id in circuit_ids:
            data = self.scrape_circuit(circuit_id)
            if data:
                results.append(data)

        _save_raw("wikipedia_circuits.json", results)
        logger.success(f"Wikipedia: {len(results)}/{len(circuit_ids)} circuitos coletados")
        return results

    # ── Helpers de parsing ────────────────────────────────────────────────────

    def _parse_length(self, fields: dict) -> float | None:
        """Extrai o comprimento em km. Ex: '5.278 km (3.279 mi)' → 5.278"""
        for key in ["length", "circuit length", "length of circuit"]:
            if key in fields:
                match = re.search(r"([\d.]+)\s*km", fields[key])
                if match:
                    return float(match.group(1))
        return None

    def _parse_int(self, fields: dict, keys: list[str]) -> int | None:
        """Extrai um inteiro de um campo. Ex: '16 corners' → 16"""
        for key in keys:
            if key in fields:
                match = re.search(r"(\d+)", fields[key])
                if match:
                    return int(match.group(1))
        return None

    def _parse_circuit_type(self, fields: dict) -> str | None:
        """Identifica se é circuito permanente ou de rua."""
        for key in ["surface", "type", "circuit type"]:
            if key in fields:
                val = fields[key].lower()
                if "street" in val or "rua" in val or "city" in val:
                    return "street"
                if "permanent" in val or "race" in val:
                    return "permanent"
        return None

    def _parse_first_gp(self, fields: dict) -> int | None:
        """Extrai o ano do primeiro GP. Ex: 'Formula One: 1973' → 1973"""
        for key in ["first race", "first grand prix", "formula one", "grands prix held"]:
            if key in fields:
                match = re.search(r"(19|20)\d{2}", fields[key])
                if match:
                    return int(match.group(0))
        return None


# ─── 2. RaceFans — manchetes por GP ──────────────────────────────────────────

# ─── Façade ───────────────────────────────────────────────────────────────────
 
class Scraper:
    """
    Ponto de entrada unificado para o pipeline.py.
 
    Uso:
        scraper  = Scraper()
        circuits = scraper.get_circuits(["monaco", "monza", "spa"])
    """
 
    def __init__(self):
        self.wikipedia = WikipediaCircuitScraper()
        #self.racefans  = RaceFansScraper()
 
    def get_circuits(self, circuit_ids: list[str]) -> list[dict]:
        return self.wikipedia.scrape_all(circuit_ids)

# ─── Exemplo de uso ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logger.remove()
    logger.add(sys.stdout, level="INFO",
               format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}")
 
    scraper = Scraper()
 
    # Wikipedia
    circuits = scraper.get_circuits(["monaco", "monza", "spa"])
    print(f"\nCircuitos coletados: {len(circuits)}")
    for c in circuits:
        print(f"  {c['circuit_id']}: {c['length_km']} km | "
              f"{c['corners']} curvas | {c['drs_zones']} DRS")