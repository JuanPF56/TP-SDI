"""Script para comparar resultados de diferentes clientes."""

import re
import json
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


def compare_all_results(results_by_client):
    """Compara los resultados de todos los clientes y devuelve las discrepancias."""
    client_discrepancies = defaultdict(lambda: {"missing": [], "anomalous": []})
    query_ids = set()

    # Recolectar todos los query_id
    for results in results_by_client.values():
        for obj in results:
            query_ids.add(obj["query_id"])

    for qid in query_ids:
        seen = {}  # client_id -> result

        for client_id, responses in results_by_client.items():
            result_obj = next(
                (obj for obj in responses if obj["query_id"] == qid), None
            )

            if result_obj is None:
                client_discrepancies[client_id]["missing"].append(qid)
                continue

            result = result_obj["results"]

            # Comparar contra los resultados ya vistos
            mismatch_found = False
            for other_result in seen.values():
                if not compare_results(result, other_result):
                    mismatch_found = True
                    break

            if mismatch_found:
                client_discrepancies[client_id]["anomalous"].append(qid)

            seen[client_id] = result

    return client_discrepancies


def compare_results(r1, r2):
    """Compara los resultados, teniendo en cuenta posibles diferencias de orden."""
    if isinstance(r1, list) and isinstance(r2, list):
        try:
            return sorted(r1) == sorted(r2)
        except TypeError:
            # Si los elementos no son ordenables (por ej. dicts), comparamos directamente
            return r1 == r2
    elif isinstance(r1, dict) and isinstance(r2, dict):
        return r1 == r2
    else:
        return r1 == r2


def main():
    folder_path = "resultados"
    results = load_results_from_folder(folder_path)
    discrepancies = compare_all_results(results)

    if not discrepancies:
        print("✅ Todos los resultados son consistentes entre los clientes.")
    else:
        print("❌ Se encontraron discrepancias por cliente:")
        for client_id, issues in discrepancies.items():
            print(f"- Cliente {client_id}:")
            if issues["missing"]:
                print("   - Missing:")
                for qid in issues["missing"]:
                    print(f"     • {qid}")
            if issues["anomalous"]:
                print("   - Anormal results:")
                for qid in issues["anomalous"]:
                    print(f"     • {qid}")


if __name__ == "__main__":
    main()
