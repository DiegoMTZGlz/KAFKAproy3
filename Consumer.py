import tkinter as tk
from tkinter import ttk
import threading
from confluent_kafka import Consumer

consumer = None

def consume_from_kafka(topic):
    global consumer
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'my_consumer_group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([topic])

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Error al consumir mensaje: {}".format(msg.error()))
            continue

        print('Mensaje recibido del tópico "{}": {}'.format(topic, msg.value().decode('utf-8')))

    consumer.close()

def detener_consumo():
    global consumer
    if consumer is not None:
        print("Consumo detenido")

def iniciar_consumo_clima():
    threading.Thread(target=consume_from_kafka, args=("clima",), daemon=True).start()

def iniciar_consumo_covid():
    threading.Thread(target=consume_from_kafka, args=("covid",), daemon=True).start()

# Configuración de la ventana
ventana = tk.Tk()
ventana.title("Consumer")
ventana.geometry("300x200")

# Botones de iniciar consumo
boton_iniciar_clima = ttk.Button(ventana, text="Iniciar Consumo de Clima", command=iniciar_consumo_clima)
boton_iniciar_clima.pack(pady=5)

boton_iniciar_covid = ttk.Button(ventana, text="Iniciar Consumo de COVID-19", command=iniciar_consumo_covid)
boton_iniciar_covid.pack(pady=5)

# Botón de detener consumo
boton_detener = ttk.Button(ventana, text="Detener Consumo", command=detener_consumo)
boton_detener.pack(pady=5)

# Ejecutar la aplicación
ventana.mainloop()
