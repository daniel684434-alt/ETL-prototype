-Prototipo ETL — Modelo Dimensional de Ventas

Este repositorio contiene un prototipo de pipeline ETL desarrollado en Python sobre el dataset Online Retail de UCI. El objetivo es construir un Data Warehouse dimensional funcional en SQLite que sirva como base de práctica para talleres de modelado de datos.

-Contexto del proyecto
El proceso de negocio analizado es ventas de comercio electrónico. El dataset cubre transacciones reales de una tienda en línea del Reino Unido entre diciembre de 2010 y diciembre de 2011, con más de 500 mil registros de líneas de factura.

El modelo sigue la metodología de Kimball. El grano de la tabla de hechos es una línea de producto dentro de una factura, registrada en una fecha y hora determinada, asociada a un cliente.

-Modelo dimensional

El esquema estrella está compuesto por cuatro dimensiones y una tabla de hechos central.
DIM_FECHA almacena las fechas descompuestas en año, trimestre, mes, semana y día, incluyendo los nombres en texto para facilitar los reportes.
DIM_HORA contiene las combinaciones de hora y minuto registradas, junto con un atributo de franja horaria que agrupa los registros en madrugada, mañana, tarde y noche.
DIM_PRODUCTO guarda el código de inventario y la descripción de cada artículo.
DIM_CLIENTE incluye el identificador del cliente, el país de origen, la región geográfica derivada y el tipo de cliente según si está registrado en el sistema o no.
FACT_VENTAS centraliza las métricas: cantidad, precio unitario, valor bruto calculado e indicador de cancelación. Se conecta a las cuatro dimensiones mediante claves sustitutas enteras.

-Resultados de ejecución

La siguiente tabla resume lo que produjo el pipeline sobre el dataset completo.
TablaRegistrosDIM_FECHA305DIM_HORA774DIM_PRODUCTO4,070DIM_CLIENTE4,373FACT_VENTAS541,909
Las validaciones de integridad no encontraron nulos en ninguna clave foránea. El valor bruto coincide exactamente con el producto de cantidad por precio unitario en todos los registros. Se identificaron 9,288 transacciones canceladas, que representan el 1.7% de las operaciones y un valor negativo de GBP 896,812 sobre un ingreso bruto total de GBP 10,644,560.

-Requisitos

Python 3.10 o superior. Las dependencias están listadas en requirements.txt.
pip install -r requirements.txt

-Cómo ejecutar

Primero descarga el dataset desde el repositorio de UCI o desde Kaggle y coloca el archivo en la carpeta data con el nombre online_retail.csv o Online Retail.xlsx. Luego ejecuta el script desde la raíz del proyecto.
python src/etl_dw_ventas.py
Al terminar, la base de datos queda disponible en output/dw_ventas.db. Puede abrirse con cualquier cliente SQLite, incluyendo DB Browser for SQLite o la extensión SQLite Viewer de VS Code.

-Dataset fuente

Online Retail Dataset publicado por el UCI Machine Learning Repository.
Chen, D., Sain, S., y Guo, K. (2012). Data mining for the online retail industry.
https://archive.ics.uci.edu/dataset/352/online+retail
