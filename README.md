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
    4. Top 10 de actores con mayor participación en películas de producción Argentina con fecha de estreno posterior al 2000
    5. Average de la tasa ingreso/presupuesto de peliculas con overview de sentimiento positivo vs. sentimiento negativo

### No funcionales

- El sistema debe estar optimizado para entornos multicomputadoras
- Se debe soportar el incremento de los elementos de cómputo para escalar los volúmenes de información a procesar
- Se requiere del desarrollo de un Middleware para abstraer la comunicación basada en grupos.
- Se debe soportar una única ejecución del procesamiento y proveer graceful quit frente a señales SIGTERM.

## Comandos

```bash
make docker-compose-up

make docker-compose-logs

make docker-compose-down
```

## Construido con 🛠️

- [Python](https://www.python.org/)
- [Docker](https://www.docker.com/)
- [RabbitMQ](https://www.rabbitmq.com/)
- [Makefile](https://www.gnu.org/software/make/manual/make.html)

## Autores ✒️

- **Juan Pablo Fresia** - 102.396 - [JuanPF56](https://github.com/JuanPF56)
- **Nathalia Lucia Encinoza Vilela** - 106.295 - [nathencinoza](https://github.com/nathencinoza)
- **Camila Belén Sebellin** - 100.204 - [camiSebe](https://github.com/camiSebe)

## Documentación 📑

- [Informe](https://docs.google.com/document/d/18aTTPUsk92PdTrNy6LHbvxGXs0G7jUu8EUrdss36D48/edit?usp=sharing)
- [Diagramas](https://drive.google.com/file/d/15dcFuXlb_mMzxmrfxLuxFFdnBSae8ah3/view?usp=sharing)
