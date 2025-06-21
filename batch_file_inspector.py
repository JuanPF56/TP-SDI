#!/usr/bin/env python3
"""
Batch File Inspector and Repair Tool

This script helps diagnose and potentially repair corrupted batch files
by analyzing their structure and attempting to fix common issues.
"""

import json
import os
import sys
from pathlib import Path
from typing import Dict, Any, List
import argparse


def inspect_json_file(file_path: Path) -> Dict[str, Any]:
    """
    Inspect a JSON file and return information about its structure.
    """
    info = {
        "file_path": str(file_path),
        "file_size": 0,
        "is_readable": False,
        "is_valid_json": False,
        "structure": None,
        "errors": [],
        "data": None,
    }

    try:
        # Check file size
        info["file_size"] = file_path.stat().st_size

        # Try to read the file
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
            info["is_readable"] = True

            # Try to parse JSON
            try:
                data = json.loads(content)
                info["is_valid_json"] = True
                info["data"] = data
                info["structure"] = analyze_structure(data)
            except json.JSONDecodeError as e:
                info["errors"].append(f"JSON decode error: {e}")
                # Try to fix common JSON issues
                fixed_content = attempt_json_repair(content)
                if fixed_content:
                    try:
                        data = json.loads(fixed_content)
                        info["is_valid_json"] = True
                        info["data"] = data
                        info["structure"] = analyze_structure(data)
                        info["errors"].append("JSON was repaired successfully")
                    except:
                        info["errors"].append("JSON repair failed")

    except Exception as e:
        info["errors"].append(f"File read error: {e}")

    return info


def analyze_structure(data: Any, path: str = "root") -> Dict[str, Any]:
    """
    Analyze the structure of data recursively.
    """
    if isinstance(data, dict):
        return {
            "type": "dict",
            "keys": list(data.keys()),
            "children": {
                k: analyze_structure(v, f"{path}.{k}") for k, v in data.items()
            },
        }
    elif isinstance(data, list):
        return {
            "type": "list",
            "length": len(data),
            "item_types": [type(item).__name__ for item in data[:5]],  # First 5 items
            "children": [
                analyze_structure(item, f"{path}[{i}]")
                for i, item in enumerate(data[:3])
            ],  # First 3 items
        }
    else:
        return {
            "type": type(data).__name__,
            "value": str(data)[:100] if len(str(data)) > 100 else str(data),
        }


def attempt_json_repair(content: str) -> str:
    """
    Attempt to repair common JSON issues.
    """
    # Remove trailing commas
    content = content.replace(",}", "}").replace(",]", "]")

    # Try to fix incomplete JSON by adding missing closing braces/brackets
    open_braces = content.count("{") - content.count("}")
    open_brackets = content.count("[") - content.count("]")

    if open_braces > 0:
        content += "}" * open_braces
    if open_brackets > 0:
        content += "]" * open_brackets

    return content


def validate_batch_structure(data: Dict[str, Any]) -> List[str]:
    """
    Validate if the data matches expected BatchMessage structure.
    """
    issues = []

    required_fields = [
        "message_id",
        "message_code",
        "client_id",
        "current_batch",
        "is_last_batch",
    ]

    for field in required_fields:
        if field not in data:
            issues.append(f"Missing required field: {field}")

    if "processed_data" in data:
        processed_data = data["processed_data"]
        if not isinstance(processed_data, list):
            issues.append(
                f"processed_data should be a list, got {type(processed_data)}"
            )
        else:
            for i, item in enumerate(processed_data):
                if not isinstance(item, dict):
                    issues.append(
                        f"processed_data[{i}] should be a dict, got {type(item)}"
                    )
                    break  # Don't report too many similar issues

    return issues


def print_structure(structure: Dict[str, Any], indent: int = 0) -> None:
    """
    Pretty print the data structure.
    """
    prefix = "  " * indent

    if structure["type"] == "dict":
        print(f"{prefix}Dict with keys: {structure['keys']}")
        for key, child in structure["children"].items():
            print(f"{prefix}  {key}:")
            print_structure(child, indent + 2)
    elif structure["type"] == "list":
        print(
            f"{prefix}List with {structure['length']} items, types: {structure['item_types']}"
        )
        for i, child in enumerate(structure["children"]):
            print(f"{prefix}  [{i}]:")
            print_structure(child, indent + 2)
    else:
        print(f"{prefix}{structure['type']}: {structure['value']}")


def inspect_directory(directory: Path) -> None:
    """
    Inspect all JSON files in a directory.
    """
    print(f"\nInspecting directory: {directory}")
    print("=" * 60)

    # Find all JSON files
    json_files = list(directory.glob("*.json"))
    corrupted_files = (
        list((directory / "corrupted").glob("*.json"))
        if (directory / "corrupted").exists()
        else []
    )

    print(
        f"Found {len(json_files)} JSON files and {len(corrupted_files)} corrupted files"
    )

    all_files = json_files + corrupted_files

    for file_path in sorted(all_files):
        print(f"\n--- {file_path.name} ---")
        info = inspect_json_file(file_path)

        print(f"Size: {info['file_size']} bytes")
        print(f"Readable: {info['is_readable']}")
        print(f"Valid JSON: {info['is_valid_json']}")

        if info["errors"]:
            print("Errors:")
            for error in info["errors"]:
                print(f"  - {error}")

        if info["structure"]:
            print("Structure:")
            print_structure(info["structure"])

            # Validate BatchMessage structure
            if info["data"]:
                issues = validate_batch_structure(info["data"])
                if issues:
                    print("BatchMessage validation issues:")
                    for issue in issues:
                        print(f"  - {issue}")
                else:
                    print("✓ Structure looks valid for BatchMessage")


def main():
    parser = argparse.ArgumentParser(description="Inspect and analyze batch JSON files")
    parser.add_argument("path", help="Path to directory or file to inspect")
    parser.add_argument(
        "--repair", action="store_true", help="Attempt to repair corrupted files"
    )

    args = parser.parse_args()

    path = Path(args.path)

    if not path.exists():
        print(f"Error: Path {path} does not exist")
        sys.exit(1)

    if path.is_file():
        print(f"Inspecting file: {path}")
        info = inspect_json_file(path)

        print(f"Size: {info['file_size']} bytes")
        print(f"Readable: {info['is_readable']}")
        print(f"Valid JSON: {info['is_valid_json']}")

        if info["errors"]:
            print("Errors:")
            for error in info["errors"]:
                print(f"  - {error}")

        if info["structure"]:
            print("Structure:")
            print_structure(info["structure"])

    elif path.is_dir():
        # Look for client directories
        client_dirs = [
            d for d in path.iterdir() if d.is_dir() and not d.name.startswith(".")
        ]

        if client_dirs:
            for client_dir in client_dirs:
                inspect_directory(client_dir)
        else:
            # Assume this is a client directory itself
            inspect_directory(path)

    print("\n" + "=" * 60)
    print("Inspection complete!")


if __name__ == "__main__":
    main()
