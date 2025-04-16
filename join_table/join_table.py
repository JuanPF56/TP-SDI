import configparser
from common.logger import get_logger

logger = get_logger("Join-Table")

# Test batches

movies = {
    {
        "id": 1,
        "original_title": "El Secreto de Sus Ojos",
    },
    {
        "id": 2,
        "original_title": "Esperando la Carroza",
    },
    {
        "id": 3,
        "original_title": "Relatos Salvajes",
    },
    {
        "id": 4,
        "original_title": "Nueve Reinas",
    },
    {
        "id": 5,
        "original_title": "La Odisea de los Giles",
    },
    {
        "id": 6,
        "original_title": "Mi obra maestra",
    },        
}

def load_config():
    config = configparser.ConfigParser()
    config.read("config.ini")
    return config

def main():
    config = load_config()
    logger.info("Join Table node is online")
    

if __name__ == "__main__":
    main()
