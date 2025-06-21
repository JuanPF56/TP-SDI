"""Receives results from the server and processes them."""

import threading
import json
import time

from protocol_client_gateway import ProtocolClient, ServerNotConnectedError
from result_presenter import ResultPresenter
from common.logger import get_logger

logger = get_logger("Results Receiver")

GATEWAY_WAIT_TIME = 5  # seconds to wait for the gateway to respond


class ResultReceiver(threading.Thread):
    def __init__(self, protocol: ProtocolClient, answers_expected: int, client_id: str):
        super().__init__(daemon=True)
        self.protocol = protocol
        self.answers_expected = answers_expected
        self._stop_flag = threading.Event()
        self.answers_received = 0
        self.client_id = client_id
        self.output_file = f"resultados/resultados_{self.client_id}.txt"
        self.presenter = ResultPresenter(client_id)
        self.querys_received = {"Q1": 0, "Q2": 0, "Q3": 0, "Q4": 0, "Q5": 0}
        self._received_hashes = set()

        # Clean up previous results
        with open(self.output_file, "w", encoding="utf-8") as f:
            f.write("")  # Clear the file

    def run(self):
        logger.info("Starting result receiver thread...")
        try:
            while not self._stop_flag.is_set():
                result = self.protocol.receive_query_response()
                logger.debug("Received result: %r", result)
                if result is None:
                    logger.warning("Received None result, gateway may be down.")
                    time.sleep(GATEWAY_WAIT_TIME)  # Avoid busy waiting
                    continue

                query = result.get("query_id")
                if query not in self.querys_received:
                    logger.error("Received unknown query ID: %s", query)
                    continue

                # Compute a simple hashable identifier (query + result content string)
                result_id = f"{query}:{json.dumps(result, sort_keys=True)}"

                if result_id in self._received_hashes:
                    logger.warning(
                        "Respuesta recibida previamente, no imprimiendo de nuevo."
                    )
                    continue

                self._received_hashes.add(result_id)
                self.querys_received[query] += 1

                if query == "Q1":
                    self.presenter.print_movies_with_genres(result)
                if query == "Q2":
                    self.presenter.print_top_spenders(result)
                if query == "Q3":
                    self.presenter.print_rating_extremes(result)
                if query == "Q4":
                    self.presenter.print_top_actors(result)
                if query == "Q5":
                    self.presenter.print_income_ratio_by_sentiment(result)

                self.append_raw_result(result)
                self.answers_received += 1

                if self.answers_received >= self.answers_expected:
                    logger.info("All expected results received.")
                    self._stop_flag.set()
                    break

        except ServerNotConnectedError:
            logger.error("Server not connected. Exiting thread.")
            self._stop_flag.set()

        except Exception as e:
            logger.error("An error occurred while receiving results: %s", e)
            self._stop_flag.set()

    def stop(self):
        self._stop_flag.set()

    def append_raw_result(self, result: dict):
        with open(self.output_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(result, ensure_ascii=False, indent=2) + "\n\n")
