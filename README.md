# TP-SDI

Trabajo Práctico Grupo 3 - Materia Sistemas Distribuidos I - FIUBA

## Requerimientos

### Funcionales

- Se solicita un sistema distribuido que analice la información de películas y los ratings de sus espectadores en plataformas como iMDb.
- Los ratings son un valor numérico de 1 al 5. Las películas tienen información como género, fecha de estreno, países involucrados en la producción, idioma, presupuesto e ingreso.
- Se debe obtener:
    1. Películas y sus géneros de los años 2000 con producción Argentina y Española.
    2. Top 5 de países que más dinero han invertido en producciones sin colaborar con otros países.
    3. Película de producción Argentina estrenada a partir del 2000, con mayor y con menor promedio de rating.
    4. Top 10 de actores con mayor participación en películas de producción Argentina con fecha de estreno posterior al 2000.
    5. Promedio de la tasa ingreso/presupuesto de películas con overview de sentimiento positivo vs. negativo.

### No funcionales

- El sistema debe estar optimizado para entornos multicomputadoras.
- Debe soportar el escalado horizontal al incrementar nodos de cómputo.
- Se requiere el desarrollo de un Middleware para abstraer la comunicación basada en grupos.
- Debe soportar una única ejecución del procesamiento y permitir un apagado limpio ante señales `SIGTERM`.
- Soporte para varias ejecuciones de las consultas por parte de un cliente, sin reinicio del servidor.
- Ejecución con varios clientes de forma concurrente.
- Correcta limpieza de los recursos luego de cada ejecución.
- Se asume que no existen fallas de los sistemas.

---

## Comandos

### ⚙️ Configurar cantidad de nodos

Antes de generar el archivo docker-compose.yaml, podés editar el archivo `global_config.ini` para ajustar la cantidad de nodos que tendrá cada componente del sistema:

```ini
[DEFAULT]

cleanup_filter_nodes = 2
production_filter_nodes = 2
year_filter_nodes = 2
sentiment_analyzer_nodes = 5
join_credits_nodes = 2
join_ratings_nodes = 3
```

🔁 Una vez configurado, ejecutá el generador de docker-compose para que los cambios se reflejen en la definición del sistema.

---

### 🔧 Generar el `docker-compose.yaml`

El sistema cuenta con un script auxiliar para facilitar la generación del archivo `docker-compose.yaml` de forma dinámica, según los parámetros que definas.

#### 📦 Instalar dependencias

Antes de ejecutar cualquier script Python, asegurate de instalar las dependencias necesarias:

```bash
pip install -r requirements.txt
```

#### ✅ Uso recomendado con `generate-compose.sh`

```bash
./generate-compose.sh <output_file.yml> [-test <test_config.yaml>] [-cant_clientes N]
```

#### 📌 Parámetros

- `<output_file.yml>`: nombre del archivo de salida (`docker-compose.yaml`, por ejemplo).

- `-test <test_config.yaml>`: opcional. Monta datasets reducidos para pruebas rápidas (`./datasets_for_test:/datasets`) y ejecuta automáticamente `download_datasets.py --test <test_config.yaml>`, donde el archivo YAML indica el porcentaje de cada dataset a usar.

- `-cant_clientes N`: opcional. Define la cantidad de clientes (client_X) que se generan en el sistema.

#### 🧪 Ejemplos

- Generar configuración completa:

```bash
./generate-compose.sh docker-compose.yaml
```

- Generar para pruebas rápidas con una config YAML:

```bash
./generate-compose.sh docker-compose.yaml -test test_config.yaml
```

- Generar con 4 clientes:

```bash
./generate-compose.sh docker-compose.yaml -cant_clientes 4
```

- Combinar ambos:

```bash
./generate-compose.sh docker-compose.yaml -test test_config.yaml -cant_clientes 2
```

> 💡 Internamente, este script llama a `download_datasets.py` con el flag `-test <test_config.yaml>` y luego ejecuta `docker-compose-generator.py`.

---

### 🧪 Preparar datasets de prueba

```bash
python3 download_datasets.py [-test <test_config.yaml>]
```

- Por defecto descarga el dataset completo desde Kaggle.
- Si se pasa el flag `--test`, los archivos se recortan según los porcentajes definidos en el YAML.
- Los archivos se guardan en la carpeta `./datasets_for_test`.

**Ejemplo de `test_config.yaml` con todos los datasets al 20%:**:

```yaml
movies_metadata.csv: 20
credits.csv: 20
ratings.csv: 20
```

---

### ▶️ Correr el sistema

Los siguientes comandos permiten levantar el entorno completo con Docker:

```bash
make docker-compose-up         # Levanta el sistema
make docker-compose-logs       # Muestra los logs
make docker-compose-down       # Detiene y elimina contenedores
```

---

## 📊 Monitoreo de las colas (RabbitMQ)

Podés visualizar el estado de las **queues** y monitorear la actividad del sistema accediendo al panel de administración de **RabbitMQ** desde tu navegador:

🔗 [http://localhost:15672/#/queues](http://localhost:15672/#/queues)

- **Usuario**: `guest`  
- **Contraseña**: `guest`

Desde este panel vas a poder inspeccionar los mensajes en las colas, ver estadísticas en tiempo real y comprobar que los workers estén procesando correctamente.

---

## 🛠️ Construido con

- [Python](https://www.python.org/)
- [Docker](https://www.docker.com/)
- [RabbitMQ](https://www.rabbitmq.com/)
- [Makefile](https://www.gnu.org/software/make/manual/make.html)
- [kagglehub](https://github.com/Kaggle/kagglehub)

---

## ✒️ Autores

- **Juan Pablo Fresia** - 102.396 - [JuanPF56](https://github.com/JuanPF56)
- **Nathalia Lucia Encinoza Vilela** - 106.295 - [nathencinoza](https://github.com/nathencinoza)
- **Camila Belén Sebellin** - 100.204 - [camiSebe](https://github.com/camiSebe)

---

## 📑 Documentación

- [Informe](https://docs.google.com/document/d/18aTTPUsk92PdTrNy6LHbvxGXs0G7jUu8EUrdss36D48/edit?usp=sharing)
- [Diagramas](https://drive.google.com/file/d/15dcFuXlb_mMzxmrfxLuxFFdnBSae8ah3/view?usp=sharing)
