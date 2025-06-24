"""Script para comparar resultados de diferentes clientes."""

import re
import json
import argparse
from pathlib import Path
from collections import defaultdict


def load_multiple_json_objects(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        text = f.read()

    # Divide entre objetos JSON usando una heurística: cada objeto empieza con {
    json_texts = re.findall(r"{.*?}(?=\s*{|\s*$)", text, flags=re.DOTALL)

    parsed = []
    for jt in json_texts:
        try:
            parsed.append(json.loads(jt))
        except json.JSONDecodeError as e:
            print(f"⚠️ Error en objeto JSON: {e}")

    return parsed


def load_results_from_folder(folder_path):
    folder = Path(folder_path)
    results_by_client = defaultdict(list)

    for file in folder.glob("*.txt"):
        client_id = file.stem.replace("resultados_", "")
        objs = load_multiple_json_objects(file)

        for obj in objs:
            if "query_id" in obj:
                results_by_client[client_id].append(obj)

    return results_by_client


def load_reference_results(reference_file):
    objs = load_multiple_json_objects(reference_file)
    reference = {}
    for obj in objs:
        if "query_id" in obj:
            reference[obj["query_id"]] = obj["results"]
    return reference


def compare_with_reference(results_by_client, reference_results):
    client_discrepancies = {}

    for client_id, responses in results_by_client.items():
        anomalous = []
        missing = []

        seen_query_ids = set()

        for obj in responses:
            qid = obj["query_id"]
            seen_query_ids.add(qid)

            if qid not in reference_results:
                print(f"⚠️ El query_id {qid} no está en los resultados de referencia.")
                continue

            expected = reference_results[qid]
            actual = obj["results"]

            if not compare_results(expected, actual):
                anomalous.append(qid)

        missing_qids = set(reference_results.keys()) - seen_query_ids
        missing.extend(missing_qids)

        if anomalous or missing:
            client_discrepancies[client_id] = {
                "anomalous": anomalous,
                "missing": list(missing),
            }

    return client_discrepancies


def normalize_result(r):
    if isinstance(r, list):
        return sorted(
            [normalize_result(x) for x in r],
            key=lambda x: json.dumps(x, sort_keys=True),
        )
    elif isinstance(r, dict):
        return {k.strip(): normalize_result(v) for k, v in sorted(r.items())}
    elif isinstance(r, float):
        return round(r, 4)  # ajustá el nivel de precisión que quieras tolerar
    elif isinstance(r, str):
        return r.strip()
    else:
        return r


def compare_results(r1, r2):
    norm_r1 = normalize_result(r1)
    norm_r2 = normalize_result(r2)

    if norm_r1 != norm_r2:
        print("⚠️ Diferencia detectada:")
        print("➡️ Esperado:")
        print(json.dumps(norm_r1, indent=2, ensure_ascii=False, sort_keys=True))
        print("➡️ Obtenido:")
        print(json.dumps(norm_r2, indent=2, ensure_ascii=False, sort_keys=True))
        print("-" * 80)
        return False
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Comparar resultados contra referencia."
    )
    parser.add_argument(
        "-20", dest="dataset_20", action="store_true", help="Usar dataset de 20"
    )
    parser.add_argument(
        "-100", dest="dataset_100", action="store_true", help="Usar dataset de 100"
    )
    parser.add_argument(
        "--folder", default="resultados", help="Carpeta con resultados de los clientes"
    )

    args = parser.parse_args()

    if args.dataset_20:
        reference_file = "resources/answers_datasets_20/resultados.txt"
    elif args.dataset_100:
        reference_file = "resources/answers_datasets_100/resultados.txt"
    else:
        print("⚠️ Debes indicar -20 o -100 para seleccionar el dataset de referencia.")
        return

    reference_results = load_reference_results(reference_file)
    client_results = load_results_from_folder(args.folder)

    discrepancies = compare_with_reference(client_results, reference_results)

    if not discrepancies:
        print("✅ Todos los resultados son consistentes con los de referencia.")
    else:
        print("❌ Se encontraron discrepancias con los resultados de referencia:")
        for client_id, issues in discrepancies.items():
            print(f"- Cliente {client_id}:")
            if issues["missing"]:
                print("   - Missing:")
                for qid in issues["missing"]:
                    print(f"     • {qid}")
            if issues["anomalous"]:
                print("   - Anomalous results:")
                for qid in issues["anomalous"]:
                    print(f"     • {qid}")


if __name__ == "__main__":
    main()
