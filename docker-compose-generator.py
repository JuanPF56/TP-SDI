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
        "build": "./gateway",
        "container_name": "gateway",
        "entrypoint": "python3 /gateway.py",
        "volumes": [
            "./gateway/config.ini:/config.ini"
        ],
        "depends_on": ["rabbitmq"],
        "networks": ["testing_net"]
    }

    # Filter nodes (reuse base image)
    for subtype in ["cleanup", "year", "production"]:
        services[f"filter_{subtype}"] = {
            "build": {
                "context": f"./filter/{subtype}",
                "dockerfile": "Dockerfile"
            },
            "container_name": f"filter_{subtype}",
            "entrypoint": "python3 /filter.py",
            "volumes": [
                f"./filter/{subtype}/config.ini:/config.ini"
            ],
            "depends_on": ["gateway"],
            "networks": ["testing_net"]
        }

    # Sentiment analyzer node
    services["sentiment_analyzer"] = {
        "build": "./sentiment_analyzer",
        "container_name": "sentiment_analyzer",
        "entrypoint": "python3 /sentiment_analyzer.py",
        "volumes": [
            "./sentiment_analyzer/config.ini:/config.ini"
        ],
        "depends_on": ["gateway"],
        "networks": ["testing_net"]
    }

    # Join table node
    services["join_table"] = {
        "build": "./join_table",
        "container_name": "join_table",
        "entrypoint": "python3 /join_table.py",
        "volumes": [
            "./join_table/config.ini:/config.ini"
        ],
        "depends_on": ["gateway"],
        "networks": ["testing_net"]
    }

    # Join batch nodes
    for subtype in ["credits", "ratings"]:
        services[f"join_batch_{subtype}"] = {
            "build": {
                "context": f"./join_batch/{subtype}",
                "dockerfile": "Dockerfile"
            },
            "container_name": f"join_batch_{subtype}",
            "entrypoint": "python3 /join_batch.py",
            "volumes": [
                f"./join_batch/{subtype}/config.ini:/config.ini"
            ],
            "depends_on": ["gateway"],
            "networks": ["testing_net"]
        }

    # Query nodes Q1 to Q5
    for i in range(1, 6):
        qname = f"q{i}"
        services[qname] = {
            "build": f"./queries/{qname}",
            "container_name": qname,
            "entrypoint": f"python3 /{qname}.py",
            "volumes": [
                f"./queries/{qname}/config.ini:/config.ini"
            ],
            "depends_on": ["gateway"],
            "networks": ["testing_net"]
        }

    services[f"client"] = {
            "build": "./client",
            "container_name": f"client",
            "entrypoint": "python3 /client.py",
            "volumes": [
                "./client/config.ini:/config.ini",
            ],
            "depends_on": ["gateway"],
            "networks": ["testing_net"]
        }

    # Compose root
    compose = {
        "version": "3.8",
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
        yaml.dump(compose, f)

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 docker-compose-generator.py <output_file.yml>")
        sys.exit(1)
    generate_compose(sys.argv[1])

if __name__ == "__main__":
    main()
