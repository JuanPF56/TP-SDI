"Generates a Docker Compose file for a distributed system with multiple nodes."

import os
import shutil
import sys
import configparser
from copy import deepcopy
import yaml

SHARD_ID_LIMIT_CREDITS = 450000  # Límite exclusivo
SHARD_ID_LIMIT_RATINGS = 26024290  # Límite exclusivo

def calculate_shard_ranges(count, is_ratings_join=False):

    if is_ratings_join and count > 1:
        # Rangos específicos para 6 nodos de ratings
        if count == 8:
            predefined_ranges = [
                (0, 586),      # Nodo 1
                (587, 1345),   # Nodo 2
                (1346, 2611),  # Nodo 3
                (2612, 4466),  # Nodo 4
                (4467, 20000), # Nodo 5 - dividiendo el rango grande
                (20001, 36529), # Nodo 6 - segunda parte del rango grande
                (36530, 106400), # Nodo 7 - dividiendo el último rango
                (106401, 176271) # Nodo 8 - segunda parte del último rango
        ]
            return predefined_ranges

        # Para otros casos (4 o menos nodos), usar distribución automática
        elif count <= 4:
            total_range = SHARD_ID_LIMIT_CREDITS + 1
            
            # If 4 or fewer nodes, distribute equally
            base_size = total_range // count
            remainder = total_range % count
            
            ranges = []
            current_start = 0
            
            for i in range(count):
                extra = 1 if i < remainder else 0
                current_end = current_start + base_size + extra - 1
                ranges.append((current_start, current_end))
                current_start = current_end + 1
                
            return ranges
        else:
            # First 4 nodes get 5% each, remaining nodes share the 80%
            small_shard_size = total_range // 300  # 5% each
            used_by_small_shards = 4 * small_shard_size  # 20% total
            remaining_range = total_range - used_by_small_shards  # 80% left
            remaining_nodes = count - 4
            base_size = remaining_range // remaining_nodes
            remainder = remaining_range % remaining_nodes
            
            ranges = []
            current_start = 0
            
            # First 4 shards get 5% each
            for i in range(4):
                current_end = current_start + small_shard_size - 1
                ranges.append((current_start, current_end))
                current_start = current_end + 1
            
            # Remaining nodes share the 80%
            for i in range(remaining_nodes):
                extra = 1 if i < remainder else 0
                current_end = current_start + base_size + extra - 1
                ranges.append((current_start, current_end))
                current_start = current_end + 1
                
            return ranges

    else:
        total_range = SHARD_ID_LIMIT_CREDITS  + 1
        first_shard_size = total_range // 10  # First shard gets 10%
        second_shard_size = total_range // 4  # Second shard gets 25%
        remaining_range = total_range - (first_shard_size + second_shard_size)
        base_size = remaining_range // (count - 2) if count > 2 else 0
        remainder = remaining_range % (count - 2) if count > 2 else 0

        ranges = []
        current_start = 0

        # First shard
        current_end = current_start + first_shard_size - 1
        ranges.append((current_start, current_end))
        current_start = current_end + 1

        # Second shard
        current_end = current_start + second_shard_size - 1
        ranges.append((current_start, current_end))
        current_start = current_end + 1

        # Remaining shards
        for i in range(count - 2):
            extra = 1 if i < remainder else 0
            current_end = current_start + base_size + extra - 1
            ranges.append((current_start, current_end))
            current_start = current_end + 1

        return ranges

def generate_system_compose(filename="docker-compose.system.yml"):
    """
    Generates a Docker Compose file based on the configuration in global_config.ini.
    :param filename: The name of the output Docker Compose file.
    """

    # Get amount of nodes
    config = configparser.ConfigParser()
    try:
        config.read("global_config.ini")
    except FileNotFoundError:
        print("Error: global_config.ini not found.")
        sys.exit(1)

    gateway_nodes = config["DEFAULT"].getint("gateway_nodes", 1)
    gateway_node_names = [f"gateway_{i}" for i in range(1, gateway_nodes + 1)]
    gateway_depends = {
        name: {"condition": "service_healthy"} for name in gateway_node_names
    }

    cleanup = config["DEFAULT"].getint("cleanup_filter_nodes", 1)
    year = config["DEFAULT"].getint("year_filter_nodes", 1)
    production = config["DEFAULT"].getint("production_filter_nodes", 1)
    sentiment_analyzer = config["DEFAULT"].getint("sentiment_analyzer_nodes", 1)
    j_credits = config["DEFAULT"].getint("join_credits_nodes", 1)
    j_ratings = config["DEFAULT"].getint("join_ratings_nodes", 1)

    services = {}

    # RabbitMQ
    services["rabbitmq"] = {
        "image": "rabbitmq:3-management",
        "container_name": "rabbitmq",
        "ports": ["5672:5672", "15672:15672"],
        "volumes": [
            "./rabbitmq/definitions.json:/etc/rabbitmq/definitions.json",
            "./rabbitmq/rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf",
        ],
        "networks": ["testing_net"],
        "healthcheck": {
            "test": ["CMD", "rabbitmq-diagnostics", "ping"],
            "interval": "5s",
            "timeout": "5s",
            "retries": 5,
        },
        "logging": {"driver": "none"},
    }

    # Filter nodes
    filter_node_names = []
    j_nodes = [f"join_credits_{i}" for i in range(1, j_credits + 1)] + [
        f"join_ratings_{i}" for i in range(1, j_ratings + 1)
    ]

    election_port_start = 9000

    # Filter nodes
    for subtype, count, await_count in [
        ("cleanup", cleanup, 1),
        ("year", year, production),
        ("production", production, cleanup),
    ]:
        depends = {
            "rabbitmq": {"condition": "service_healthy"},
            **gateway_depends,
        }
        if subtype == "year":
            for node in j_nodes:
                depends[node] = {"condition": "service_started"}

        peer_list = [
            f"filter_{subtype}_{j}:{election_port_start + j}"
            for j in range(1, count + 1)
        ]
        peers_str = ",".join(peer_list)

        for i in range(1, count + 1):
            name = f"filter_{subtype}_{i}"
            filter_node_names.append(name)
            services[name] = {
                "container_name": name,
                "image": f"filter_{subtype}:latest",
                "entrypoint": "python3 /app/filter.py",
                "volumes": [
                    "/var/run/docker.sock:/var/run/docker.sock",
                    f"./filter/{subtype}/config.ini:/app/config.ini",
                    f"./filter/{subtype}/storage:/app/storage",
                ],
                "environment": {
                    "NODE_ID": str(i),
                    "NODE_TYPE": subtype,
                    "NODES_TO_AWAIT": str(await_count),
                    "NODES_OF_TYPE": count,
                    "NODE_NAME": name,
                    "ELECTION_PORT": str(election_port_start + i),
                    "PEERS": peers_str,
                },
                "depends_on": deepcopy(depends),
                "networks": ["testing_net"],
            }

        election_port_start += count  # 🔧 Avoid port overlap

    # Sentiment analyzer nodes
    sentiment_node_names = []
    peer_list = [
        f"sentiment_analyzer_{j}:{election_port_start + j}"
        for j in range(1, sentiment_analyzer + 1)
    ]
    peers_str = ",".join(peer_list)

    for i in range(1, sentiment_analyzer + 1):
        name = f"sentiment_analyzer_{i}"
        sentiment_node_names.append(name)
        services[name] = {
            "container_name": name,
            "image": "sentiment_analyzer:latest",
            "entrypoint": "python3 /app/sentiment_analyzer.py",
            "volumes": [
                "/var/run/docker.sock:/var/run/docker.sock",
                "./sentiment_analyzer/config.ini:/app/config.ini",
                "./sentiment_analyzer/storage:/app/storage",
            ],
            "environment": {
                "NODE_ID": str(i),
                "NODE_TYPE": "sentiment_analyzer",
                "NODES_TO_AWAIT": str(cleanup),
                "NODES_OF_TYPE": sentiment_analyzer,
                "NODE_NAME": name,
                "ELECTION_PORT": str(election_port_start + i),
                "PEERS": peers_str,
            },
            "depends_on": {
                "rabbitmq": {"condition": "service_healthy"},
                **gateway_depends,
            },
            "networks": ["testing_net"],
        }

    election_port_start += sentiment_analyzer

    # Join nodes
    join_node_names = []
    for typ, count in [("credits", j_credits), ("ratings", j_ratings)]:
        peer_list = [
            f"join_{typ}_{j}:{election_port_start + j}" for j in range(1, count + 1)
        ]
        peers_str = ",".join(peer_list)
        shard_ranges = calculate_shard_ranges(count, typ == "ratings")
        
        # Create shard mapping for this type
        shard_map = {}
        for i in range(1, count + 1):
            node_name = f"join_{typ}_{i}"
            start_range, end_range = shard_ranges[i - 1]
            shard_map[node_name] = f"{start_range}-{end_range}"
        
        # Convert shard map to string (similar to peers_str)
        shard_mapping_str = ",".join([f"{name}:{range_str}" for name, range_str in shard_map.items()])

        for i in range(1, count + 1):
            name = f"join_{typ}_{i}"
            join_node_names.append(name)
            start_range, end_range = shard_ranges[i - 1]

            services[name] = {
                "container_name": name,
                "image": f"join_{typ}:latest",
                "entrypoint": "python3 /app/join.py",
                "volumes": [
                    "/var/run/docker.sock:/var/run/docker.sock",
                    f"./join/{typ}/config.ini:/app/config.ini",
                    f"./join/{typ}/storage:/app/storage",
                ],
                "environment": {
                    "NODE_ID": str(i),
                    "NODE_TYPE": f"join_{typ}",
                    "NODES_TO_AWAIT": str(cleanup),
                    "NODES_OF_TYPE": count,
                    "YEAR_NODES_TO_AWAIT": str(year),
                    "NODE_NAME": name,
                    "ELECTION_PORT": str(election_port_start + i),
                    "PEERS": peers_str,
                    "SHARD_MAPPING": shard_mapping_str,  # New: complete shard mapping
                    "SHARD_RANGE_START": str(start_range),  # Keep individual range for convenience
                    "SHARD_RANGE_END": str(end_range),
                },
                "depends_on": {
                    "rabbitmq": {"condition": "service_healthy"},
                    **gateway_depends,
                },
                "networks": ["testing_net"],
            }
    # Nodes to await by query
    query_node_names = []
    nodes_to_await = {
        "q1": year,
        "q2": production,
        "q3": j_ratings,
        "q4": j_credits,
        "q5": sentiment_analyzer,
    }

    # Query nodes Q1 to Q5
    for i in range(1, 6):
        qname = f"q{i}"
        query_node_names.append(qname)
        services[qname] = {
            "container_name": qname,
            "image": f"query_{qname}:latest",
            "entrypoint": f"python3 /app/{qname}.py",
            "volumes": [
                "/var/run/docker.sock:/var/run/docker.sock",
                f"./query/{qname}/config.ini:/app/config.ini",
                f"./query/{qname}/storage:/app/storage",
            ],
            "environment": {
                "NODE_TYPE": qname,
                "NODES_TO_AWAIT": str(nodes_to_await[qname]),
                "NODE_NAME": qname,
            },
            "depends_on": {
                "rabbitmq": {"condition": "service_healthy"},
                **gateway_depends,
            },
            "networks": ["testing_net"],
        }

    # proxy node for gateways
    services["proxy"] = {
        "container_name": "proxy",
        "image": "proxy:latest",
        "volumes": [
            "/var/run/docker.sock:/var/run/docker.sock",
            "./proxy/config.ini:/app/config.ini",
        ],
        "environment": {
            "NODE_NAME": "proxy",
        },
        "networks": ["testing_net"],
        "ports": ["8000:8000", "9000:9000"],
    }

    # Gateway nodes
    full_dependencies = (
        filter_node_names + sentiment_node_names + join_node_names + query_node_names
    )
    for i, name in enumerate(gateway_node_names, start=1):
        services[name] = {
            "container_name": name,
            "image": "gateway:latest",
            "entrypoint": "python3 /app/main.py",
            "volumes": [
                "/var/run/docker.sock:/var/run/docker.sock",
                "./gateway/config.ini:/app/config.ini",
                "./gateway/storage:/app/storage",
            ],
            "environment": {
                "SYSTEM_NODES": ",".join(full_dependencies),
                "NODE_NAME": name,
                "GATEWAY_PORT": f"{9000+i}",
            },
            "depends_on": {
                "rabbitmq": {"condition": "service_healthy"},
                "proxy": {"condition": "service_started"},
            },
            "networks": ["testing_net"],
            "ports": [f"{9000+i}:{9000+i}"],
            "healthcheck": {
                "test": ["CMD", "test", "-f", f"/tmp/{name}_ready"],
                "interval": "5s",
                "timeout": "5s",
                "retries": 10,
            },
        }

    # Coordinator node
    monitored_nodes = (
        filter_node_names
        + sentiment_node_names
        + join_node_names
        + query_node_names
        + gateway_node_names
        + ["proxy"]
    )
    depends_coordinator = {
        node: {"condition": "service_started"} for node in monitored_nodes
    }
    services["coordinator"] = {
        "container_name": "coordinator",
        "image": "coordinator:latest",
        "entrypoint": "python3 /app/coordinator.py",
        "volumes": [
            "/var/run/docker.sock:/var/run/docker.sock",
            "./coordinator/coordinator.py:/app/coordinator.py",
            "./filter/cleanup/storage:/app/storage/filter_cleanup",
            "./filter/year/storage:/app/storage/filter_year",
            "./filter/production/storage:/app/storage/filter_production",
            "./sentiment_analyzer/storage:/app/storage/sentiment_analyzer",
            "./join/credits/storage:/app/storage/join_credits",
            "./join/ratings/storage:/app/storage/join_ratings",
        ],
        "environment": {
            "MONITORED_NODES": ",".join(monitored_nodes),
        },
        "depends_on": {**gateway_depends, **depends_coordinator},
        "networks": ["testing_net"],
    }

    compose = {
        "services": services,
        "networks": {
            "testing_net": {
                "ipam": {
                    "driver": "default",
                    "config": [{"subnet": "172.25.125.0/24"}],
                },
                "external": True,
            }
        },
    }

    with open(filename, "w", encoding="utf-8") as f:
        yaml.dump(compose, f, sort_keys=False)


def generate_clients_compose(
    filename="docker-compose.clients.yml", short_test=False, cant_clientes=1
):
    """
    Generates a Docker Compose file for client nodes.
    :param filename: The name of the output Docker Compose file.
    :param short_test: If True, uses a shorter test dataset.
    :param cant_clientes: The number of client nodes to create.
    """
    services = {}

    for i in range(1, cant_clientes + 1):
        client_volumes = [
            "./client/config.ini:/app/config.ini",
            "./resultados:/app/resultados",
            "./data:/data",
        ]

        services[f"client_{i}"] = {
            "container_name": f"client_{i}",
            "image": "client:latest",
            "entrypoint": "python3 /app/main.py",
            "volumes": client_volumes,
            "environment": {
                "USE_TEST_DATASET": "1" if short_test else "0",
            },
            "networks": ["testing_net"],
        }

    compose = {
        "services": services,
        "networks": {"testing_net": {"external": True}},
    }

    with open(filename, "w", encoding="utf-8") as f:
        yaml.dump(compose, f, sort_keys=False)


def clean_resultados_folder():
    """
    Deletes all files and directories in the resultados directory.
    """
    resultados_path = "./resultados"
    if os.path.exists(resultados_path):
        for filename in os.listdir(resultados_path):
            file_path = os.path.join(resultados_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)  # eliminar archivo o link
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)  # eliminar carpeta
                print(f"Deleted: {file_path}")
            except Exception as e:
                print(f"Failed to delete {file_path}. Reason: {e}")
    else:
        print(f"Result directory does not exist: {resultados_path}")


def clean_storage_folder():
    """
    Deletes all files and directories in the storage directory.
    """
    storage_path = "./gateway/storage"
    if os.path.exists(storage_path):
        for filename in os.listdir(storage_path):
            file_path = os.path.join(storage_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)  # eliminar archivo o link
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)  # eliminar carpeta
                print(f"Deleted: {file_path}")
            except Exception as e:
                print(f"Failed to delete {file_path}. Reason: {e}")
    else:
        print(f"Result directory does not exist: {storage_path}")


def main():
    """
    Main function to handle command line arguments and generate the appropriate Docker Compose file.
    """
    args = sys.argv[1:]
    if not args:
        print("Usage:")
        print("  python3 compose_generator.py system <filename>")
        print(
            "  python3 compose_generator.py clients <filename> [-test] [-cant_clientes N]"
        )
        sys.exit(1)

    # Clean old resultados and storage folders before generating compose files
    clean_resultados_folder()
    clean_storage_folder()

    mode = args[0]
    if mode == "system":
        filename = args[1] if len(args) > 1 else "docker-compose.system.yml"
        generate_system_compose(filename)

    elif mode == "clients":
        filename = args[1] if len(args) > 1 else "docker-compose.clients.yml"
        short_test = "-test" in args
        cant_clientes = 1
        if "-cant_clientes" in args:
            idx = args.index("-cant_clientes")
            if idx + 1 < len(args):
                cant_clientes = int(args[idx + 1])
        generate_clients_compose(filename, short_test, cant_clientes)

    else:
        print(f"Unknown mode: {mode}")
        sys.exit(1)


if __name__ == "__main__":
    main()
