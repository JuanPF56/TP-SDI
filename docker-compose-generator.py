"Generates a Docker Compose file for a distributed system with multiple nodes."

import sys
import configparser
from copy import deepcopy
import yaml


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

    for subtype, count, await_count in [
        ("cleanup", cleanup, 1),
        ("year", year, production),
        ("production", production, cleanup),
    ]:
        depends = {
            "rabbitmq": {"condition": "service_healthy"},
            "gateway": {"condition": "service_healthy"},
        }
        if subtype == "year":
            for node in j_nodes:
                depends[node] = {"condition": "service_started"}

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
                ],
                "environment": {
                    "NODE_ID": str(i),
                    "NODE_TYPE": subtype,
                    "NODES_TO_AWAIT": str(await_count),
                    "NODES_OF_TYPE": count,
                    "NODE_NAME": name,
                },
                "depends_on": deepcopy(depends),
                "networks": ["testing_net"],
            }

    # Sentiment analyzer node
    sentiment_node_names = []
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
            ],
            "environment": {
                "NODE_ID": str(i),
                "NODE_TYPE": "sentiment_analyzer",
                "NODES_TO_AWAIT": str(cleanup),
                "NODES_OF_TYPE": sentiment_analyzer,
                "NODE_NAME": name,
            },
            "depends_on": {
                "rabbitmq": {"condition": "service_healthy"},
                "gateway": {"condition": "service_healthy"},
            },
            "networks": ["testing_net"],
        }

    # Join nodes
    join_node_names = []
    for typ, count in [("credits", j_credits), ("ratings", j_ratings)]:
        for i in range(1, count + 1):
            name = f"join_{typ}_{i}"
            join_node_names.append(name)
            services[name] = {
                "container_name": name,
                "image": f"join_{typ}:latest",
                "entrypoint": "python3 /app/join.py",
                "volumes": [
                    "/var/run/docker.sock:/var/run/docker.sock",
                    f"./join/{typ}/config.ini:/app/config.ini",
                ],
                "environment": {
                    "NODE_ID": str(i),
                    "NODE_TYPE": f"join_{typ}",
                    "NODES_TO_AWAIT": str(cleanup),
                    "NODES_OF_TYPE": count,
                    "YEAR_NODES_TO_AWAIT": str(year),
                    "NODE_NAME": name,
                },
                "depends_on": {
                    "rabbitmq": {"condition": "service_healthy"},
                    "gateway": {"condition": "service_healthy"},
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
            ],
            "environment": {
                "NODE_TYPE": qname,
                "NODES_TO_AWAIT": str(nodes_to_await[qname]),
                "NODE_NAME": qname,
            },
            "depends_on": {
                "rabbitmq": {"condition": "service_healthy"},
                "gateway": {"condition": "service_healthy"},
            },
            "networks": ["testing_net"],
        }

    # Gateway node
    full_dependencies = (
        filter_node_names + sentiment_node_names + join_node_names + query_node_names
    )
    services["gateway"] = {
        "container_name": "gateway",
        "image": "gateway:latest",
        "entrypoint": "python3 /app/main.py",
        "volumes": [
            "/var/run/docker.sock:/var/run/docker.sock",
            "./gateway/config.ini:/app/config.ini",
            "./resultados:/app/resultados",
        ],
        "environment": {
            "SYSTEM_NODES": ",".join(full_dependencies),
        },
        "depends_on": {
            "rabbitmq": {"condition": "service_healthy"},
        },
        "networks": ["testing_net"],
        "healthcheck": {
            "test": ["CMD", "test", "-f", "/tmp/gateway_ready"],
            "interval": "5s",
            "timeout": "5s",
            "retries": 10,
        },
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
