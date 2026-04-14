import os
import requests
import pandas as pd
from io import StringIO                          # ✅ FIX 1: Importado correctamente
from bs4 import BeautifulSoup                   # ✅ FIX 2: Importado correctamente
from flask import Flask, render_template, jsonify
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)

# ─────────────────────────────────────────────
# Configuración de Base de Datos
# ─────────────────────────────────────────────
# os.environ.get busca una variable del sistema llamada DATABASE_URL.
# Si no existe (en local), usa SQLite como base de datos de prueba.
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', 'sqlite:///local.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# ─────────────────────────────────────────────
# Modelo de la Base de Datos
# ─────────────────────────────────────────────
# Esto define la "tabla" en la base de datos.
# Cada atributo es una columna.
class PrecioDiario(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    fecha = db.Column(db.Date, nullable=False)
    sector = db.Column(db.String(50), nullable=False)
    g90 = db.Column(db.Float)
    g95 = db.Column(db.Float)
    g97 = db.Column(db.Float)
    db5 = db.Column(db.Float)
    fuente = db.Column(db.String(20), default='scraping')
    # 'csv_historico' = vino del CSV de datosabiertos.gob.pe
    # 'scraping'      = vino del scraping diario de preciocombustible.com

# ─────────────────────────────────────────────
# Mapeo de Distritos a Sectores de Lima
# ─────────────────────────────────────────────
SECTORES_MAP = {
    'Lima Centro': ['LIMA', 'JESUS MARIA', 'LA VICTORIA', 'BREÑA', 'LINCE', 'SAN MIGUEL', 'PUEBLO LIBRE', 'MAGDALENA', 'SAN ISIDRO', 'MIRAFLORES', 'SURQUILLO', 'SAN BORJA', 'SANTIAGO DE SURCO'],
    'Cono Norte': ['LOS OLIVOS', 'COMAS', 'INDEPENDENCIA', 'SAN MARTIN DE PORRES', 'PUENTE PIEDRA', 'ANCON', 'CARABAYLLO'],
    'Cono Sur': ['SAN JUAN DE MIRAFLORES', 'VILLA EL SALVADOR', 'VILLA MARIA DEL TRIUNFO', 'CHORRILLOS', 'LURIN', 'PACHACAMAC', 'SAN BARTOLO', 'PUNTA NEGRA', 'PUNTA HERMOSA'],
    'Cono Este': ['SAN JUAN DE LURIGANCHO', 'ATE', 'SANTA ANITA', 'EL AGUSTINO', 'RIMAC', 'SAN LUIS', 'CIENEGUILLA', 'CHACLACAYO', 'LA MOLINA']
}

PRODUCTOS_OBJETIVO = ['GASOHOL 90', 'GASOHOL 95', 'GASOHOL 97', 'DB5 S-50']

# ─────────────────────────────────────────────
# Función principal de scraping
# ─────────────────────────────────────────────
def obtener_datos_osinergmin():
    """Lee precios por grifo desde tablas[1] de preciocombustible.com
    y calcula promedios por sector de Lima"""

    url = "https://www.preciocombustible.com/lima/"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept-Language': 'es-PE,es;q=0.9',
    }

    try:
        # ── BLOQUE 1: Descargar la página ──
        print(">>> Conectando a preciocombustible.com...")
        response = requests.get(url, headers=headers, timeout=15)
        print(f">>> Status: {response.status_code}")
        response.raise_for_status()

        # ── BLOQUE 2: Leer todas las tablas ──
        tablas = pd.read_html(StringIO(response.text))
        print(f">>> Tablas encontradas: {len(tablas)}")

        # tablas[1] tiene el detalle por grifo y distrito
        df = tablas[1]
        print(f">>> Filas en tablas[1]: {len(df)}")

        # ── BLOQUE 3: Quedarnos solo con las columnas que usaremos ──
        # Renombramos las columnas largas a nombres cortos y simples
        # Piénsalo como ponerle apodos a columnas con nombres muy largos
        df = df.rename(columns={
            'Distrito':                    'distrito',
            'Producto':                    'producto',
            'Precio de Venta por Galón':   'precio',
            'Fecha de Registro del Precio':'fecha_precio',
        })

        # Nos quedamos solo con esas 4 columnas, ignoramos el resto
        df = df[['distrito', 'producto', 'precio', 'fecha_precio']]
        print(f">>> Columnas seleccionadas: {df.columns.tolist()}")

        # ── BLOQUE 4: Convertir precios de centavos a soles ──
        # Los precios vienen como 1365 → dividimos entre 100 → 13.65
        df['precio'] = pd.to_numeric(df['precio'], errors='coerce') / 100
        # errors='coerce' convierte valores raros (texto, vacíos) a NaN
        # en lugar de lanzar un error — como tachar lo que no sirve

        # ── BLOQUE 5: Estandarizar nombres de productos ──
        # El sitio usa nombres distintos a los nuestros.
        # Creamos un "diccionario de traducción"
        mapa_productos = {
            'GASOHOL REGULAR':   'GASOHOL 90',
            'GASOHOL PREMIUM':   'GASOHOL 95',
            'GASOHOL SUPERIOR':  'GASOHOL 97',
            'Diesel B5 S-50':    'DB5 S-50',
            'Diesel B5 S-50 UV': 'DB5 S-50',   # variante, la unimos
        }
        # Reemplazamos los nombres originales por los nuestros
        # Si un producto no está en el mapa, lo dejamos como está
        df['producto'] = df['producto'].replace(mapa_productos)

        # Filtramos solo los 4 productos que nos interesan
        productos_objetivo = ['GASOHOL 90', 'GASOHOL 95', 'GASOHOL 97', 'DB5 S-50']
        df = df[df['producto'].isin(productos_objetivo)]
        print(f">>> Filas después de filtrar productos: {len(df)}")

        # ── BLOQUE 6: Convertir distritos a MAYÚSCULAS ──
        # Nuestro SECTORES_MAP usa mayúsculas ('COMAS', 'ATE', etc.)
        # Así evitamos que 'Comas' ≠ 'COMAS' cause problemas
        df['distrito'] = df['distrito'].str.upper().str.strip()

        # ── BLOQUE 7: Asignar cada distrito a su sector ──
        # Creamos una función que busca el sector de un distrito
        def buscar_sector(distrito):
            for sector, distritos in SECTORES_MAP.items():
                if distrito in distritos:
                    return sector
            return None  # Si no lo encontramos, devolvemos None

        df['sector'] = df['distrito'].apply(buscar_sector)
        # .apply() aplica la función fila por fila — como usar la función
        # en cada celda de la columna, una a una

        # Eliminamos filas donde el distrito no está en ningún sector
        df = df.dropna(subset=['sector'])
        print(f">>> Filas con sector reconocido: {len(df)}")

        # ── BLOQUE 8: Calcular promedio por sector y producto ──
        # Agrupamos: para cada combinación (sector + producto),
        # calculamos el precio promedio de todos los grifos de esa zona
        promedios = df.groupby(['sector', 'producto'])['precio'].mean()
        # El resultado es una tabla con índice (sector, producto) → precio_promedio

        print(f">>> Promedios calculados:\n{promedios}")
        return promedios

    except Exception as e:
        print(f">>> Error en scraping: {e}")
        import traceback
        traceback.print_exc()   # Muestra exactamente en qué línea falló
        return None


# ─────────────────────────────────────────────
# Guardar datos en la base de datos
# ─────────────────────────────────────────────
def actualizar_base_datos():
    """Verifica si hay datos de hoy, si no, hace scraping y guarda"""
    hoy = datetime.now().date()
    existe = PrecioDiario.query.filter_by(fecha=hoy).first()

    if not existe:
        print(f"No hay datos de hoy ({hoy}). Obteniendo datos...")
        promedios = obtener_datos_osinergmin()

        if promedios is not None:
            for sector in SECTORES_MAP.keys():
                # ── Leer cada precio del promedio calculado ──
                # .get() busca el valor en el índice doble (sector, producto)
                # Si no existe, devuelve 0 en lugar de lanzar error
                def precio(producto):
                    try:
                        return round(float(promedios.loc[(sector, producto)]), 2)
                    except KeyError:
                        return 0  # Si no hay datos de ese producto en ese sector

                nuevo_registro = PrecioDiario(
                    fecha=hoy,
                    sector=sector,
                    g90=precio('GASOHOL 90'),
                    g95=precio('GASOHOL 95'),
                    g97=precio('GASOHOL 97'),
                    db5=precio('DB5 S-50'),
                )
                db.session.add(nuevo_registro)

            db.session.commit()
            print("✅ Datos guardados correctamente.")
        else:
            print("⚠️ No se pudieron obtener datos.")

# ─────────────────────────────────────────────
# Rutas de la aplicación
# ─────────────────────────────────────────────
# @app.route define qué URL activa cada función.
# '/' es la página principal.
@app.route('/')
def home():
    return render_template('index.html')


# '/api/datos' es el endpoint que devuelve los datos en formato JSON
# para que el frontend los use en los gráficos
@app.route('/api/datos')
def api_datos():
    # Se ejecuta cada vez que se piden datos
    # para asegurar que estén actualizados
    actualizar_base_datos()

    # Obtener los últimos 6 meses de historial
    hace_6_meses = datetime.now() - timedelta(days=180)
    registros = PrecioDiario.query.filter(
        PrecioDiario.fecha >= hace_6_meses
    ).order_by(PrecioDiario.fecha.asc()).all()

    # Formatear para el frontend
    # Convertimos cada registro de la BD a un diccionario (JSON)
    datos_formateados = []
    for reg in registros:
        datos_formateados.append({
            "fecha": reg.fecha.strftime('%Y-%m-%d'),
            "sector": reg.sector,
            "precios": {
                "g90": reg.g90,
                "g95": reg.g95,
                "g97": reg.g97,
                "db5": reg.db5
            }
        })
    return jsonify(datos_formateados)


# ─────────────────────────────────────────────
# Iniciar la aplicación
# ─────────────────────────────────────────────
# Esto crea las tablas en la BD si no existen todavía
with app.app_context():
    db.create_all()

# debug=True muestra errores detallados en el navegador (solo en desarrollo)
if __name__ == '__main__':
    app.run(debug=True)

# ─────────────────────────────────────────────
# SCHEDULER: Scraping automático diario
# ─────────────────────────────────────────────
# APScheduler es como un despertador dentro de la app.
# BackgroundScheduler corre en segundo plano sin
# bloquear las peticiones normales de la web.
def tarea_diaria():
    """Función que se ejecuta automáticamente cada día"""
    print("⏰ Scheduler: ejecutando scraping diario...")
    with app.app_context():
        # Reutilizamos la misma función que ya tenemos
        actualizar_base_datos()

# Creamos el scheduler y le decimos cada cuánto correr
scheduler = BackgroundScheduler(timezone="America/Lima")
scheduler.add_job(
    func=tarea_diaria,
    trigger='cron',      # 'cron' significa "a una hora fija"
    hour=8,              # 8am hora Lima
    minute=0,
    id='scraping_diario',
    replace_existing=True
)
scheduler.start()
print("✅ Scheduler iniciado — scraping diario a las 8:00am Lima")

# ─────────────────────────────────────────────
# Iniciar la aplicación
# ─────────────────────────────────────────────
with app.app_context():
    db.create_all()

if __name__ == '__main__':
    app.run(debug=True)