import multiprocessing
import subprocess

def iniciar_producer():
    subprocess.run(["python", "Producer.py"])

def iniciar_consumer():
    subprocess.run(["python", "Consumer.py"])

if __name__ == "__main__":
    p1 = multiprocessing.Process(target=iniciar_producer)
    p2 = multiprocessing.Process(target=iniciar_consumer)

    p1.start()
    p2.start()

    p1.join()
    p2.join()
