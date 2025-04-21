import threading
import json

from protocol_client_gateway import ProtocolClient
from common.logger import get_logger

logger = get_logger("Results Receiver")

class ResultReceiver(threading.Thread):
    def __init__(self, protocol: ProtocolClient, answers_expected: int):
        super().__init__(daemon=True)
        self.protocol = protocol
        self.answers_expected = answers_expected
        self._stop_flag = threading.Event()
        self.answers_received = 0

        # Clean up previous results
        with open("resultados/resultados.txt", "w", encoding="utf-8") as f:
            f.write("") # Clear the file

    def run(self):
        logger.info("Starting result receiver thread...")
        while not self._stop_flag.is_set():
            result = self.protocol.receive_query_response()
            if result is not None:
                if result:
                    if result.get("query") == "Q1":
                        pretty_print_movies_with_genres(result)
                    if result.get("query") == "Q2":
                        pretty_print_top_spenders(result)
                    if result.get("query") == "Q3":
                        pretty_print_rating_extremes(result)
                    if result.get("query") == "Q4":
                        pretty_print_top_actors(result)
                    if result.get("query") == "Q5":
                        pretty_print_income_ratio_by_sentiment(result)
                append_raw_result(result)
                self.answers_received += 1
                if self.answers_received >= self.answers_expected:
                    logger.info("All expected results received.")
                    self._stop_flag.set()
                    break

    def stop(self):
        self._stop_flag.set()


def pretty_print_movies_with_genres(result: dict):
    query_id = result.get("query", "Q?")
    rows = result.get("results", [])

    lines = []
    if not rows:
        lines.append(f"No se encontraron resultados para la consulta {query_id}")
    else:
        lines.append(f"Resultados de la consulta {query_id}: Películas y sus géneros de los años 2000 con producción Argentina y Española")
        lines.append("-" * 75)
        lines.append(f"{'#':<3} {'Película':35} | Géneros")
        lines.append("-" * 75)
        for idx, (title, genres) in enumerate(rows, start=1):
            genre_str = ", ".join(genres)
            lines.append(f"{idx:<3} {title:35} | {genre_str}")
        lines.append("-" * 75)

    print_ascii_box(lines)


def pretty_print_top_spenders(result: dict):
    query_id = result.get("query", "Q?")
    rows = result.get("results", [])

    lines = []
    if not rows:
        lines.append(f"No se encontraron resultados para la consulta {query_id}")
    else:
        lines.append(f"Resultados de la consulta {query_id}: Top 5 de países que más dinero han invertido en producciones sin colaborar con otros países.")
        lines.append("-" * 69)
        for idx, (country, amount) in enumerate(rows, start=1):
            formatted_amount = f"${amount:,.0f}".replace(",", ".")
            lines.append(f"{idx}. {country:<28} - {formatted_amount}")
        lines.append("-" * 69)

    print_ascii_box(lines)


def pretty_print_rating_extremes(result: dict):
    query_id = result.get("query", "Q?")
    rows = result.get("results", [])

    lines = []
    if not rows or len(rows) != 2:
        lines.append(f"No se encontraron resultados para la consulta {query_id}")
    else:
        max_movie, max_rating = rows[0]
        min_movie, min_rating = rows[1]

        lines.append(f"Resultados de la consulta {query_id}: Película de producción Argentina estrenada a partir del 2000, con mayor y con menor promedio de rating.")
        lines.append("-" * 72)
        lines.append(f"🎬 Película con mayor rating promedio: {max_movie} ({max_rating:.2f})")
        lines.append(f"🎬 Película con menor rating promedio: {min_movie} ({min_rating:.2f})")
        lines.append("-" * 72)

    print_ascii_box(lines)


def pretty_print_top_actors(result: dict):
    query_id = result.get("query", "Q?")
    rows = result.get("results", [])

    lines = []
    if not rows:
        lines.append(f"No se encontraron resultados para la consulta {query_id}")
    else:
        lines.append(f"Resultados de la consulta {query_id}: Top 10 de actores con mayor participación en películas de producción Argentina con fecha de estreno posterior al 2000")
        lines.append("-" * 65)
        lines.append(f"{'Pos.':<5} {'Actor':40} Participaciones")
        lines.append("-" * 65)

        for idx, (actor, count) in enumerate(rows, start=1):
            lines.append(f"{idx:<5} {actor:40} {count}")
        lines.append("-" * 65)

    print_ascii_box(lines)


def pretty_print_income_ratio_by_sentiment(result: dict):
    query_id = result.get("query", "Q?")
    rows = result.get("results", [])

    lines = []
    if not rows or len(rows) != 2:
        lines.append(f"No se encontraron resultados para la consulta {query_id}")
    else:
        positive_ratio, negative_ratio = rows
        positive_ratio = float(positive_ratio)
        negative_ratio = float(negative_ratio)

        lines.append(f"Resultados de la consulta {query_id}: Average de la tasa ingreso/presupuesto de peliculas con overview de sentimiento positivo vs. sentimiento negativo")
        lines.append("-" * 58)
        lines.append(f"😊 Sentimiento positivo - Tasa ingreso/presupuesto: {positive_ratio:.2f}")
        lines.append(f"☹️  Sentimiento negativo - Tasa ingreso/presupuesto: {negative_ratio:.2f}")
        lines.append("-" * 58)

    print_ascii_box(lines)


def append_raw_result(result: dict):
    with open("resultados/resultados.txt", "a", encoding="utf-8") as f:
        f.write(json.dumps(result, ensure_ascii=False, indent=2) + "\n\n")


def print_ascii_box(lines: list[str]):
    max_width = max(len(line) for line in lines)
    border = "+" + "-" * (max_width + 2) + "+"
    logger.info(border)
    for line in lines:
        logger.info(f"| {line.ljust(max_width)} |")
    logger.info(border)
