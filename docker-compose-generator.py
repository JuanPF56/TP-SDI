import sys
import configparser
import yaml
import uuid
from copy import deepcopy

def generate_compose(filename, short_test=False, cant_clientes=1):

    # Get amount of nodes
    config = configparser.ConfigParser()
    try:
        config.read("global_config.ini")
    except FileNotFoundError:
        print("Error: global_config.ini not found.")
        sys.exit(1)
        
    cleanup = config["DEFAULT"].getint("cleanup_filter_nodes", 1)
    year = config["DEFAULT"].getint("year_filter_nodes", 1)
    production = config["DEFAULT"].getint("production_filter_nodes", 1)
    sentiment_analyzer = config["DEFAULT"].getint("sentiment_analyzer_nodes", 1)
    j_credits = config["DEFAULT"].getint("join_credits_nodes", 1)
    j_ratings = config["DEFAULT"].getint("join_ratings_nodes", 1)

    j_nodes = []
    for i in range(1, j_credits + 1):
        j_nodes.append(f"join_credits_{i}")
    for i in range(1, j_ratings + 1):
        j_nodes.append(f"join_ratings_{i}")

    services = {}
    
    # RabbitMQ node
    services["rabbitmq"] = {
        "image": "rabbitmq:3-management",
        "container_name": "rabbitmq",
        "ports": ["5672:5672", "15672:15672"],
        "volumes": [
            "./rabbitmq/definitions.json:/etc/rabbitmq/definitions.json",
            "./rabbitmq/rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf"
        ],
        "networks": ["testing_net"], 
        "healthcheck": {
            "test": ["CMD", "rabbitmq-diagnostics", "ping"],
            "interval": "5s",
            "timeout": "5s",
            "retries": 5
        },
        "logging": {
            "driver": "none"
        }
    }

    # Filter nodes
    depends = {}
    filter_node_names = []
    for subtype in ["cleanup", "year", "production"]:
        if subtype == "cleanup":
            num_nodes = cleanup
            nodes_to_await = 1
            depends = {"rabbitmq": {"condition": "service_healthy"}}
        elif subtype == "year":
            num_nodes = year
            nodes_to_await = production
            depends = {"rabbitmq": {"condition": "service_healthy"}}
            for node in j_nodes:
                depends[node] = {"condition": "service_started"}
        elif subtype == "production":
            num_nodes = production
            nodes_to_await = cleanup
            depends = {"rabbitmq": {"condition": "service_healthy"}}
        for i in range(1, num_nodes + 1):
            name = f"filter_{subtype}_{i}"
            filter_node_names.append(name)
            services[f"filter_{subtype}_{i}"] = {
                "container_name": f"filter_{subtype}_{i}",
                "image": f"filter_{subtype}:latest",
                "entrypoint": "python3 /app/filter.py",
                "volumes": [
                    f"./filter/{subtype}/config.ini:/app/config.ini"
                ],
                "environment": {
                    "NODE_ID": str(i),
                    "NODE_TYPE": subtype,
                    "NODES_TO_AWAIT": str(nodes_to_await),
                    "NODES_OF_TYPE": num_nodes,
                    "NODE_NAME": f"filter_{subtype}_{i}",
                },
                "depends_on": deepcopy(depends),
                "networks": ["testing_net"]
            }

    # Sentiment analyzer node
    sentiment_node_names = []
    for i in range(1, sentiment_analyzer + 1):
        name = f"sentiment_analyzer_{i}"
        sentiment_node_names.append(name)
        services[f"sentiment_analyzer_{i}"] = {
            "container_name": f"sentiment_analyzer_{i}",
            "image": "sentiment_analyzer:latest",
            "entrypoint": "python3 /app/sentiment_analyzer.py",
            "volumes": [
                "./sentiment_analyzer/config.ini:/app/config.ini"
            ],
            "environment": {
                "NODE_ID": str(i),
                "NODE_TYPE": "sentiment_analyzer",
                "NODES_TO_AWAIT": str(cleanup),
                "NODES_OF_TYPE": sentiment_analyzer,
                "NODE_NAME": f"sentiment_analyzer_{i}",
            },
            "depends_on": {
                "rabbitmq": {"condition": "service_healthy"},
            },
            "networks": ["testing_net"]
        }

    # Join nodes
    join_node_names = []
    for i in range(1, j_credits + 1):
        name = f"join_credits_{i}"
        join_node_names.append(name)
        services[f"join_credits_{i}"] = {
            "container_name": f"join_credits_{i}",
            "image": f"join_credits:latest",
            "entrypoint": "python3 /app/join.py",
            "environment": {
                "NODE_ID": str(i),
                "NODE_TYPE": "join_credits",
                "NODES_TO_AWAIT": str(cleanup),
                "NODES_OF_TYPE": j_credits,
                "YEAR_NODES_TO_AWAIT": str(year),
                "NODE_NAME": f"join_credits_{i}",
            },
            "volumes": [
                f"./join/credits/config.ini:/app/config.ini"
            ],
            "depends_on": {
                "rabbitmq": {"condition": "service_healthy"},
            },
            "networks": ["testing_net"]
        }
    for i in range(1, j_ratings + 1):
        name = f"join_ratings_{i}"
        join_node_names.append(name)
        services[f"join_ratings_{i}"] = {
            "container_name": f"join_ratings_{i}",
            "image": f"join_ratings:latest",
            "entrypoint": "python3 /app/join.py",
            "environment": {
                "NODE_ID": str(i),
                "NODE_TYPE": "join_ratings",
                "NODES_TO_AWAIT": str(cleanup),
                "NODES_OF_TYPE": j_ratings,
                "YEAR_NODES_TO_AWAIT": str(year),
                "NODE_NAME": f"join_ratings_{i}",
            },
            "volumes": [
                f"./join/ratings/config.ini:/app/config.ini"
            ],
            "depends_on": {
                "rabbitmq": {"condition": "service_healthy"},
            },
            "networks": ["testing_net"]
        }

    # Nodes to await by query
    query_node_names = []
    nodes_to_await = {
        "q1": year,
        "q2": production,
        "q3": j_ratings,
        "q4": j_credits,
        "q5": sentiment_analyzer
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
                f"./query/{qname}/config.ini:/app/config.ini"
            ],
            "environment": {
                "NODE_TYPE": qname,
                "NODES_TO_AWAIT": str(nodes_to_await[qname]),
                "NODE_NAME": qname,
            },
            "depends_on": {
                "rabbitmq": {"condition": "service_healthy"},
            },
            "networks": ["testing_net"]
        }

    # Gateway node
    full_dependencies = (filter_node_names + sentiment_node_names + join_node_names + query_node_names)
    services["gateway"] = {
        "container_name": "gateway",
        "image": "gateway:latest",
        "entrypoint": "python3 /app/main.py",
        "volumes": [
            "./gateway/config.ini:/app/config.ini",
            "./resultados:/app/resultados"
        ],
        "environment": {
            "SYSTEM_NODES": ",".join(full_dependencies),
        },
        "depends_on": {
                "rabbitmq": {"condition": "service_healthy"},
        },
        "networks": ["testing_net"]
    }

    # Clients nodes
    for i in range(1, cant_clientes + 1):
        client_volumes = [
            "./client/config.ini:/app/config.ini",
            f"./resultados:/app/resultados"
        ]
        if short_test:
            client_volumes.append("./datasets_for_test:/datasets")

        services[f"client_{i}"] = {
            "container_name": f"client_{i}",
            "image": "client:latest",
            "entrypoint": "python3 /app/main.py",
            "volumes": client_volumes,
            "environment": {
                "USE_TEST_DATASET": "1" if short_test else "0", 
                "REQUESTS_TO_SERVER": "1"
            },
            "depends_on": ["gateway"],
            "networks": ["testing_net"]
        }

    # Compose root
    compose = {
        "services": services,
        "networks": {
            "testing_net": {
                "ipam": {
                    "driver": "default",
                    "config": [{"subnet": "172.25.125.0/24"}]
                }
            }
        }
    }

    with open(filename, "w") as f:
        yaml.dump(compose, f, sort_keys=False)

def main():
    args = sys.argv[1:]

    if not args or args[0].startswith("-"):
        print("Usage: python3 docker-compose-generator.py <output_file.yml> [-test] [-cant_clientes <n>]")
        sys.exit(1)

    filename = args[0]
    short_test = "-test" in args

    # Obtener cantidad de clientes (default 1)
    cant_clientes = 1
    if "-cant_clientes" in args:
        idx = args.index("-cant_clientes")
        if idx + 1 < len(args):
            cant_clientes = int(args[idx + 1])

    generate_compose(filename, short_test, cant_clientes)

if __name__ == "__main__":
    main()