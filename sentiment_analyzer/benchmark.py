import time
from transformers import pipeline
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

TEXTS = ["This movie was absolutely fantastic!"] * 100
MODEL_NAME = "distilbert/distilbert-base-uncased-finetuned-sst-2-english"


def analyze_thread(text):
    global model
    return model(text, truncation=True)[0]["label"]


def analyze_process(text):
    pipe = pipeline("sentiment-analysis", model=MODEL_NAME)
    return pipe(text, truncation=True)[0]["label"]


def run_thread_benchmark():
    global model
    model = pipeline("sentiment-analysis", model=MODEL_NAME)
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(analyze_thread, TEXTS))
    end = time.perf_counter()
    print(f"ThreadPoolExecutor: {end - start:.2f} seconds")


def run_process_benchmark():
    start = time.perf_counter()
    with ProcessPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(analyze_process, TEXTS))
    end = time.perf_counter()
    print(f"ProcessPoolExecutor: {end - start:.2f} seconds")


if __name__ == "__main__":
    print("Running thread benchmark...")
    run_thread_benchmark()
    print("\nRunning process benchmark...")
    run_process_benchmark()
