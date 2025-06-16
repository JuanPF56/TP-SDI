"""Receives results from the server and processes them."""

import threading
import json

from protocol_client_gateway import ProtocolClient, ServerNotConnectedError
from result_presenter import ResultPresenter
from common.logger import get_logger

logger = get_logger("Results Receiver")


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

        # Clean up previous results
        with open(self.output_file, "w", encoding="utf-8") as f:
            f.write("")  # Clear the file

    def run(self):
        logger.info("Starting result receiver thread...")
        try:
            while not self._stop_flag.is_set():
                result = self.protocol.receive_query_response()
                logger.debug("Received result: %r", result)
                if result is not None:
                    if result:
                        query = result.get("query_id")
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
                else:
                    logger.warning(
                        "Received None result, possibly due to server disconnection."
                    )
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
