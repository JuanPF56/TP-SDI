import sys
import yaml

def generate_compose(filename):
    services = {}

    # RabbitMQ node
    services["rabbitmq"] = {
        "image": "rabbitmq:3-management",
        "container_name": "rabbitmq",
        "ports": ["5672:5672", "15672:15672"],
        "volumes": [
            "./rabbitmq/definitions.json:/etc/rabbitmq/definitions.json"
        ],
        "networks": ["testing_net"]
    }

    # Gateway node
    services["gateway"] = {
        "container_name": "gateway",
        "image": "gateway:latest",
        "entrypoint": "python3 /app/gateway.py",
        "volumes": [
            "./gateway/config.ini:/app/config.ini"
        ],
        "depends_on": ["rabbitmq"],
        "networks": ["testing_net"]
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
            "depends_on": ["gateway"],
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
        "depends_on": ["gateway"],
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
        "depends_on": ["gateway"],
        "networks": ["testing_net"]
    }

    # Join batch nodes
    for subtype in ["credits", "ratings"]:
        services[f"join_batch_{subtype}"] = {
            "container_name": f"join_batch_{subtype}",
            "image": f"join_batch_{subtype}:latest",
            "entrypoint": "python3 /app/join_batch.py",
            "volumes": [
                f"./join_batch/{subtype}/config.ini:/app/config.ini"
            ],
            "depends_on": ["gateway", "join_table"],
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
            "depends_on": ["gateway"],
            "networks": ["testing_net"]
        }

    # Client node
    services["client"] = {
        "container_name": "client",
        "image": "client:latest",
        "entrypoint": "python3 /app/client.py",
        "volumes": [
            "./client/config.ini:/app/config.ini"
        ],
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
    if len(sys.argv) != 2:
        print("Usage: python3 docker-compose-generator.py <output_file.yml>")
        sys.exit(1)
    generate_compose(sys.argv[1])

if __name__ == "__main__":
    main()
