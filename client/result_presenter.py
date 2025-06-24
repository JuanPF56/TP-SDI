from common.logger import get_logger

logger = get_logger("Results Presenter")


class ResultPresenter:
    def __init__(self, client_id: str):
        self.client_id = client_id

    def print_movies_with_genres(self, result: dict):
        query_id = result.get("query_id", "Q1")
        rows = result.get("results", [])

        lines = [
            f"                   CLIENT ID {self.client_id}",
            f"                              QUERY: {query_id}",
            "-" * 77,
            "Películas y sus géneros de los años 2000 con producción Argentina y Española.",
            "-" * 77,
        ]

        if not rows:
            lines.append(f"No se encontraron resultados para la consulta {query_id}")
        else:
            lines.append(f"{'#':<3} {'Película':35} | Géneros")
            lines.append("-" * 77)
            for idx, (title, genres) in enumerate(sorted(rows.items()), start=1):
                genre_str = ", ".join(genres)
                lines.append(f"{idx:<3} {title:35} | {genre_str}")
        lines.append("-" * 77)

        self._print_box(lines)

    def print_top_spenders(self, result: dict):
        query_id = result.get("query", "Q2")
        rows = result.get("results", [])

        lines = [
            f"                CLIENT ID {self.client_id}",
            f"                            QUERY: {query_id}",
            "-" * 69,
            "Top 5 de países que más dinero han invertido en producciones sin",
            "colaborar con otros países.",
            "-" * 69,
        ]

        if not rows:
            lines.append(f"No se encontraron resultados para la consulta {query_id}")
        else:
            lines.append(f"{'#':<3} {'País':28} | Dinero invertido")
            lines.append("-" * 69)
            for idx, (country, amount) in enumerate(rows, start=1):
                formatted_amount = f"$ {amount:,.0f}".replace(",", ".")
                lines.append(f"{idx}. {country:<29} | {formatted_amount}")
        lines.append("-" * 69)

        self._print_box(lines)

    def print_rating_extremes(self, result: dict):
        query_id = result.get("query", "Q3")
        res = result.get("results", {})

        lines = [
            f"                 CLIENT ID {self.client_id}",
            f"                            QUERY: {query_id}",
            "-" * 72,
            "Película de producción Argentina estrenada a partir del 2000,",
            "con mayor y con menor promedio de rating.",
            "-" * 72,
        ]

        if not res or "highest" not in res or "lowest" not in res:
            lines.append(f"No se encontraron resultados para la consulta {query_id}")
        else:
            max_movie = res["highest"].get("title", "N/A")
            max_rating = res["highest"].get("rating", 0)
            min_movie = res["lowest"].get("title", "N/A")
            min_rating = res["lowest"].get("rating", 0)

            lines.append(
                f"🎬 Película con mayor rating promedio: {max_movie} ({max_rating:.2f})"
            )
            lines.append(
                f"🎬 Película con menor rating promedio: {min_movie} ({min_rating:.2f})"
            )
            lines.append("-" * 72)

        self._print_box(lines)

    def print_top_actors(self, result: dict):
        query_id = result.get("query", "Q4")
        res = result.get("results", {})

        lines = [
            f"               CLIENT ID {self.client_id}",
            f"                            QUERY: {query_id}",
            "-" * 70,
            "Top 10 de actores con mayor participación en películas de producción",
            "Argentina con fecha de estreno posterior al 2000.",
            "Nota: En caso de empate, se ordena alfabéticamente.",
            "-" * 70,
        ]

        if not res or not res.get("actors"):
            lines.append(f"No se encontraron resultados para la consulta {query_id}")
        else:
            lines.append(f"{'Pos.':<5} {'Actor':30} Participaciones")
            lines.append("-" * 70)
            sorted_actors = sorted(
                res["actors"], key=lambda a: (-a.get("count", 0), a.get("name", ""))
            )
            for idx, actor in enumerate(sorted_actors, start=1):
                lines.append(
                    f"{idx:<5} {actor.get('name', 'N/A'):30} {actor.get('count', 0)}"
                )
        lines.append("-" * 70)

        self._print_box(lines)

    def print_income_ratio_by_sentiment(self, result: dict):
        query_id = result.get("query", "Q5")
        ratios = result.get("results", {})

        lines = [
            f"              CLIENT ID {self.client_id}",
            f"                            QUERY: {query_id}",
            "-" * 66,
            "Average de la tasa ingreso/presupuesto de peliculas con",
            "overview de sentimiento positivo vs. sentimiento negativo.",
            "-" * 66,
        ]

        if not ratios or not isinstance(ratios, dict):
            lines.append(f"No se encontraron resultados para la consulta {query_id}")
        else:
            try:
                positive_ratio = float(ratios.get("average_positive_rate", 0))
                negative_ratio = float(ratios.get("average_negative_rate", 0))

                lines.append(
                    f"😊 Sentimiento positivo - Tasa ingreso/presupuesto: {positive_ratio:.2f}"
                )
                lines.append(
                    f"☹️ Sentimiento negativo - Tasa ingreso/presupuesto: {negative_ratio:.2f}"
                )
                lines.append("-" * 66)
            except (ValueError, TypeError) as e:
                lines.append(
                    f"Error al procesar los resultados para la consulta {query_id}: {e}"
                )

        self._print_box(lines)

    def _print_box(self, lines: list[str]):
        max_width = max(len(line) for line in lines)
        border = "+" + "-" * (max_width + 2) + "+"
        logger.info(border)
        for line in lines:
            logger.info(f"| {line.ljust(max_width)} |")
        logger.info(border)
