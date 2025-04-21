import sys
import configparser
import yaml

def generate_compose(filename, short_test=False):

    # Get amount of nodes
    config = configparser.ConfigParser()
    try:
        config.read("global_config.ini")
    except FileNotFoundError:
        print("Error: global_config.ini not found.")
        sys.exit(1)
    # TODO: Add rest of node amounts
    jb_credits = config["DEFAULT"].getint("join_batch_credits_nodes", 1)
    jb_ratings = config["DEFAULT"].getint("join_batch_ratings_nodes", 1)

    services = {}
    
    # RabbitMQ node
    services["rabbitmq"] = {
        "image": "rabbitmq:3-management",
        "container_name": "rabbitmq",
        "ports": ["5672:5672", "15672:15672"],
        "volumes": [
            "./rabbitmq/definitions.json:/etc/rabbitmq/definitions.json"
        ],
        "networks": ["testing_net"], 
        "healthcheck": {
            "test": ["CMD", "rabbitmq-diagnostics", "ping"],
            "interval": "5s",
            "timeout": "5s",
            "retries": 5
        }
    }

    # Gateway node
    services["gateway"] = {
        "container_name": "gateway",
        "image": "gateway:latest",
        "entrypoint": "python3 /app/main.py",
        "volumes": [
            "./gateway/config.ini:/app/config.ini"
        ],
        "depends_on": {
            "rabbitmq": {
                "condition": "service_healthy"
            }
        },
        "networks": ["testing_net"], 
        "healthcheck": {
            "test": ["CMD", "test", "-f", "/tmp/gateway_ready"],
            "interval": "5s",
            "timeout": "5s",
            "retries": 5
        }
    }

    # Filter nodes
    for subtype in ["cleanup", "year", "production"]:
        services[f"filter_{subtype}"] = {
            "container_name": f"filter_{subtype}",
            "image": f"filter_{subtype}:latest",
            "entrypoint": "python3 /app/filter.py",
            "volumes": [
                f"./filter/{subtype}/config.ini:/app/config.ini"
            ],
            "depends_on": {
                "gateway": {
                    "condition": "service_healthy"
                }
            },
            "networks": ["testing_net"]
        }

    # Sentiment analyzer node
    services["sentiment_analyzer"] = {
        "container_name": "sentiment_analyzer",
        "image": "sentiment_analyzer:latest",
        "entrypoint": "python3 /app/sentiment_analyzer.py",
        "volumes": [
            "./sentiment_analyzer/config.ini:/app/config.ini"
        ],
        "depends_on": {
            "gateway": {
                "condition": "service_healthy"
            }
        },
        "networks": ["testing_net"]
    }

    # Join table node
    services["join_table"] = {
        "container_name": "join_table",
        "image": "join_table:latest",
        "entrypoint": "python3 /app/join_table.py",
        "volumes": [
            "./join_table/config.ini:/app/config.ini"
        ],
        "environment": {
            "JB_CREDITS_NODES": str(jb_credits),
            "JB_RATINGS_NODES": str(jb_ratings)
        },
        "depends_on": {
            "gateway": {
                "condition": "service_healthy"
            }
        },
        "networks": ["testing_net"]
    }

    # Join batch nodes
    for i in range(1, jb_credits + 1):
        services[f"join_batch_credits_{i}"] = {
            "container_name": f"join_batch_credits_{i}",
            "image": f"join_batch_credits:latest",
            "entrypoint": "python3 /app/join_batch.py",
            "environment": {
                "NODE_ID": str(i),
                "NODE_TYPE": "credits"
            },
            "volumes": [
                f"./join_batch/credits/config.ini:/app/config.ini"
            ],
            "depends_on": {
                "gateway": {
                    "condition": "service_healthy"
                }
            },
            "networks": ["testing_net"]
        }
    for i in range(1, jb_ratings + 1):
        services[f"join_batch_ratings_{i}"] = {
            "container_name": f"join_batch_ratings_{i}",
            "image": f"join_batch_ratings:latest",
            "entrypoint": "python3 /app/join_batch.py",
            "environment": {
                "NODE_ID": str(i),
                "NODE_TYPE": "ratings"
            },
            "volumes": [
                f"./join_batch/ratings/config.ini:/app/config.ini"
            ],
            "depends_on": {
                "gateway": {
                    "condition": "service_healthy"
                }
            },
            "networks": ["testing_net"]
        }

    # Query nodes Q1 to Q5
    for i in range(1, 6):
        qname = f"q{i}"
        services[qname] = {
            "container_name": qname,
            "image": f"query_{qname}:latest",
            "entrypoint": f"python3 /app/{qname}.py",
            "volumes": [
                f"./query/{qname}/config.ini:/app/config.ini"
            ],
            "depends_on": {
                "gateway": {
                    "condition": "service_healthy"
                }
            },
            "networks": ["testing_net"]
        }

    # Client node
    client_volumes = [
        "./client/config.ini:/app/config.ini",
        "./resultados:/app/resultados"
    ]
    if short_test:
        client_volumes.append("./datasets_for_test:/datasets")

    services["client"] = {
        "container_name": "client",
        "image": "client:latest",
        "entrypoint": "python3 /app/main.py",
        "volumes": client_volumes,
        "environment": {
            "USE_TEST_DATASET": "1" if short_test else "0"
        },
        "depends_on": {
            "gateway": {
                "condition": "service_healthy"
            }
        },
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
        print("Usage: python3 docker-compose-generator.py <output_file.yml> [-short_test]")
        sys.exit(1)

    filename = args[0]
    short_test = "-short_test" in args
    generate_compose(filename, short_test)

if __name__ == "__main__":
    main()
