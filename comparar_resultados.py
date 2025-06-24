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
    client_discrepancies = defaultdict(lambda: {"missing": [], "anomalous": []})

    for client_id, responses in results_by_client.items():
        seen_query_ids = set()

        for obj in responses:
            qid = obj["query_id"]
            seen_query_ids.add(qid)

            if qid not in reference_results:
                print(f"⚠️ El query_id {qid} no está en los resultados de referencia.")
                continue

            expected = reference_results[qid]
            actual = obj["results"]

            ok, diff_detail = compare_results(expected, actual)
            if not ok:
                client_discrepancies[client_id]["anomalous"].append((qid, diff_detail))

        # Verificar si el cliente omitió algún query_id esperado
        missing_qids = set(reference_results.keys()) - seen_query_ids
        client_discrepancies[client_id]["missing"].extend(missing_qids)

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


def diff_actors(expected, obtained):
    expected_map = {a["name"]: a["count"] for a in expected}
    obtained_map = {a["name"]: a["count"] for a in obtained}

    expected_names = set(expected_map.keys())
    obtained_names = set(obtained_map.keys())

    missing = expected_names - obtained_names
    extra = obtained_names - expected_names
    common = expected_names & obtained_names

    differences = []

    if missing:
        differences.append("🚫 Actores faltantes (en resultado):")
        for name in sorted(missing):
            differences.append(f"   • {name} (esperado count: {expected_map[name]})")

    if extra:
        differences.append("⚠️ Actores inesperados (en resultado):")
        for name in sorted(extra):
            differences.append(f"   • {name} (obtenido count: {obtained_map[name]})")

    for name in sorted(common):
        if expected_map[name] != obtained_map[name]:
            differences.append(
                f"🔄 Diferente count para {name}: esperado {expected_map[name]}, obtenido {obtained_map[name]}"
            )

    return differences


def compare_results(r1, r2):
    norm_r1 = normalize_result(r1)
    norm_r2 = normalize_result(r2)

    if norm_r1 != norm_r2:
        diff_detail = []

        # Si ambos son dicts y tienen clave 'actors', analizamos diferencias detalladas
        if isinstance(norm_r1, dict) and isinstance(norm_r2, dict):
            if "actors" in norm_r1 and "actors" in norm_r2:
                diffs = diff_actors(norm_r1["actors"], norm_r2["actors"])
                diff_detail.extend(diffs)

        return False, diff_detail

    return True, []


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
    if not reference_results:
        print(
            "⚠️ No se encontraron resultados de referencia en el archivo especificado."
        )
        return
    client_results = load_results_from_folder(args.folder)
    if not client_results:
        print("⚠️ No se encontraron resultados de clientes en la carpeta especificada.")
        return

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
                for qid, diff_detail in issues["anomalous"]:
                    print(f"     • {qid}")
                    if diff_detail:
                        for line in diff_detail:
                            print(f"       {line}")
            print("\n")


if __name__ == "__main__":
    main()
