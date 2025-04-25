import threading
import json

from protocol_client_gateway import ProtocolClient
import common.exceptions as exceptions

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
        try:
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
                else:
                    logger.warning("Received None result, possibly due to server disconnection.")
                    self._stop_flag.set()
                    break

        except exceptions.ServerNotConnectedError as e:
            logger.error("Server not connected. Exiting thread.")
            self._stop_flag.set()
        
        except Exception as e:
            logger.error(f"An error occurred while receiving results: {e}")
            self._stop_flag.set()

    def stop(self):
        self._stop_flag.set()


def pretty_print_movies_with_genres(result: dict):
    query_id = result.get("query", "Q?")
    rows = result.get("results", [])

    lines = []
    lines.append(f"                                   {query_id}")
    lines.append("Películas y sus géneros de los años 2000 con producción Argentina y Española.")

    if not rows:
        lines.append("-" * 77)
        lines.append("No se encontraron resultados para la consulta")
        lines.append("-" * 77)
    else:
        lines.append("-" * 77)
        lines.append(f"{'#':<3} {'Película':35} | Géneros")
        lines.append("-" * 77)
        for idx, (title, genres) in enumerate(rows, start=1):
            genre_str = ", ".join(genres)
            lines.append(f"{idx:<3} {title:35} | {genre_str}")
        lines.append("-" * 77)

    print_ascii_box(lines)


def pretty_print_top_spenders(result: dict):
    query_id = result.get("query", "Q?")
    rows = result.get("results", [])

    lines = []
    lines.append(f"                             {query_id}")
    lines.append("Top 5 de países que más dinero han invertido en producciones sin")
    lines.append("colaborar con otros países.")

    if not rows:
        lines.append("-" * 69)
        lines.append(f"No se encontraron resultados para la consulta")
        lines.append("-" * 69)
    else:
        lines.append("-" * 69)
        lines.append(f"{'#':<3} {'País':28} | Dinero invertido")
        lines.append("-" * 69)
        for idx, (country, amount) in enumerate(rows, start=1):
            formatted_amount = f"$ {amount:,.0f}".replace(",", ".")
            lines.append(f"{idx}. {country:<29} | {formatted_amount}")
        lines.append("-" * 69)

    print_ascii_box(lines)


def pretty_print_rating_extremes(result: dict):
    query_id = result.get("query", "Q?")
    res = result.get("results", {})

    lines = []
    lines.append(f"                             {query_id}")
    lines.append("-" * 72)
    lines.append("Película de producción Argentina estrenada a partir del 2000,")
    lines.append(" con mayor y con menor promedio de rating.")

    if not res or not isinstance(res, dict) or "highest" not in res or "lowest" not in res:
        lines.append("-" * 72)
        lines.append(f"No se encontraron resultados para la consulta {query_id}")
    else:
        max_movie = res["highest"].get("title", "N/A")
        max_rating = res["highest"].get("rating", 0)
        min_movie = res["lowest"].get("title", "N/A")
        min_rating = res["lowest"].get("rating", 0)

        lines.append("-" * 72)
        lines.append(f"🎬 Película con mayor rating promedio: {max_movie} ({max_rating:.2f})")
        lines.append(f"🎬 Película con menor rating promedio: {min_movie} ({min_rating:.2f})")
        lines.append("-" * 72)

    print_ascii_box(lines)

def pretty_print_top_actors(result: dict):
    query_id = result.get("query", "Q?")
    res = result.get("results", {})

    lines = []
    lines.append(f"                            {query_id}")
    lines.append("Top 10 de actores con mayor participación en películas de producción")
    lines.append("-" * 70)
    lines.append("Argentina con fecha de estreno posterior al 2000.")

    if not res or not isinstance(res, dict) or "actors" not in res:
        lines.append("-" * 70)
        lines.append(f"No se encontraron resultados para la consulta")
    else:
        lines.append("-" * 70)
        lines.append(f"{'Pos.':<5} {'Actor':30} Participaciones")
        lines.append("-" * 70)

        for idx, actor_data in enumerate(res["actors"], start=1):
            actor_name = actor_data.get("name", "N/A")
            actor_count = actor_data.get("count", 0)
            lines.append(f"{idx:<5} {actor_name:30} {actor_count}")
        lines.append("-" * 70)

    print_ascii_box(lines)



def pretty_print_income_ratio_by_sentiment(result: dict):
    query_id = result.get("query", "Q?")
    ratios = result.get("results", {})

    lines = []
    lines.append(f"                                              {query_id}")
    lines.append("-" * 62)
    lines.append("Average de la tasa ingreso/presupuesto de peliculas con overview de sentimiento positivo vs. sentimiento negativo.")

    if not ratios or not isinstance(ratios, dict):
        lines.append("-" * 62)
        lines.append(f"No se encontraron resultados para la consulta {query_id}")
    else:
        try:
            positive_ratio = float(ratios.get("average_positive_rate", 0))
            negative_ratio = float(ratios.get("average_negative_rate", 0))
            lines.append("-" * 62)
            lines.append(f"😊 Sentimiento positivo - Tasa ingreso/presupuesto: {positive_ratio:.2f}")
            lines.append(f"☹️  Sentimiento negativo - Tasa ingreso/presupuesto: {negative_ratio:.2f}")
            lines.append("-" * 62)
        except (ValueError, TypeError) as e:
            lines.append(f"Error al procesar los resultados para la consulta {query_id}: {e}")

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
