import os
import requests
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup
from flask import Flask, render_template, jsonify
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)

# ─────────────────────────────────────────────
# Configuración de Base de Datos
# ─────────────────────────────────────────────
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', 'sqlite:///local.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# FIX SSL: Configuramos cómo SQLAlchemy maneja las conexiones.
# En Render, las conexiones inactivas se cierran solas.
# pool_pre_ping=True verifica si la conexión sigue viva antes de usarla,
# como tocar la puerta antes de entrar para ver si alguien responde.
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    'pool_pre_ping': True,
    'pool_recycle': 280,
    'pool_timeout': 20,
    'pool_size': 5,
    'max_overflow': 2
}

db = SQLAlchemy(app)

# ─────────────────────────────────────────────
# Modelo de la Base de Datos
# ─────────────────────────────────────────────
class PrecioDiario(db.Model):
    id      = db.Column(db.Integer, primary_key=True)
    fecha   = db.Column(db.Date, nullable=False)
    sector  = db.Column(db.String(50), nullable=False)
    g90     = db.Column(db.Float)
    g95     = db.Column(db.Float)
    g97     = db.Column(db.Float)
    db5     = db.Column(db.Float)
    fuente  = db.Column(db.String(20), default='scraping')

# ─────────────────────────────────────────────
# Mapeo de Distritos a Sectores de Lima
# ─────────────────────────────────────────────
SECTORES_MAP = {
    'Lima Centro': ['LIMA', 'JESUS MARIA', 'LA VICTORIA', 'BREÑA', 'LINCE', 'SAN MIGUEL', 'PUEBLO LIBRE', 'MAGDALENA', 'SAN ISIDRO', 'MIRAFLORES', 'SURQUILLO', 'SAN BORJA', 'SANTIAGO DE SURCO'],
    'Cono Norte':  ['LOS OLIVOS', 'COMAS', 'INDEPENDENCIA', 'SAN MARTIN DE PORRES', 'PUENTE PIEDRA', 'ANCON', 'CARABAYLLO'],
    'Cono Sur':    ['SAN JUAN DE MIRAFLORES', 'VILLA EL SALVADOR', 'VILLA MARIA DEL TRIUNFO', 'CHORRILLOS', 'LURIN', 'PACHACAMAC', 'SAN BARTOLO', 'PUNTA NEGRA', 'PUNTA HERMOSA'],
    'Cono Este':   ['SAN JUAN DE LURIGANCHO', 'ATE', 'SANTA ANITA', 'EL AGUSTINO', 'RIMAC', 'SAN LUIS', 'CIENEGUILLA', 'CHACLACAYO', 'LA MOLINA']
}

PRODUCTOS_OBJETIVO = ['GASOHOL 90', 'GASOHOL 95', 'GASOHOL 97', 'DB5 S-50']

# ─────────────────────────────────────────────
# Función principal de scraping
# ─────────────────────────────────────────────
def obtener_datos_osinergmin():
    url = "https://www.preciocombustible.com/lima/"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept-Language': 'es-PE,es;q=0.9',
    }

    try:
        print(">>> Conectando a preciocombustible.com...")
        response = requests.get(url, headers=headers, timeout=15)
        print(f">>> Status: {response.status_code}")
        response.raise_for_status()

        tablas = pd.read_html(StringIO(response.text))
        print(f">>> Tablas encontradas: {len(tablas)}")

        df = tablas[1]
        print(f">>> Filas en tablas[1]: {len(df)}")

        df = df.rename(columns={
            'Distrito':                     'distrito',
            'Producto':                     'producto',
            'Precio de Venta por Galón':    'precio',
            'Fecha de Registro del Precio': 'fecha_precio',
        })
        df = df[['distrito', 'producto', 'precio', 'fecha_precio']]

        df['precio'] = pd.to_numeric(df['precio'], errors='coerce') / 100

        mapa_productos = {
            'GASOHOL REGULAR':   'GASOHOL 90',
            'GASOHOL PREMIUM':   'GASOHOL 95',
            'GASOHOL SUPERIOR':  'GASOHOL 97',
            'Diesel B5 S-50':    'DB5 S-50',
            'Diesel B5 S-50 UV': 'DB5 S-50',
        }
        df['producto'] = df['producto'].replace(mapa_productos)
        df = df[df['producto'].isin(PRODUCTOS_OBJETIVO)]
        print(f">>> Filas después de filtrar productos: {len(df)}")

        df['distrito'] = df['distrito'].str.upper().str.strip()

        def buscar_sector(distrito):
            for sector, distritos in SECTORES_MAP.items():
                if distrito in distritos:
                    return sector
            return None

        df['sector'] = df['distrito'].apply(buscar_sector)
        df = df.dropna(subset=['sector'])
        print(f">>> Filas con sector reconocido: {len(df)}")

        # ── BLOQUE NUEVO: Filtrar outliers y muestras pequeñas ──────────
        # Antes de promediar, limpiamos los precios "raros".
        # Analogía: si 10 grifos cobran S/15 y uno cobra S/50,
        # ese único grifo distorsionaría el promedio. Lo descartamos.

        def filtrar_outliers(grupo):
            """Elimina precios fuera del rango normal del grupo"""
            if len(grupo) < 3:
                # Menos de 3 grifos = muestra poco confiable, la ignoramos
                return grupo.iloc[0:0]  # devuelve un DataFrame vacío
            Q1 = grupo.quantile(0.25)
            Q3 = grupo.quantile(0.75)
            IQR = Q3 - Q1
            # Rango "normal": entre Q1-1.5*IQR y Q3+1.5*IQR
            return grupo[(grupo >= Q1 - 1.5 * IQR) & (grupo <= Q3 + 1.5 * IQR)]

        df_filtrado = []
        for (sector, producto), grupo in df.groupby(['sector', 'producto']):
            precios_limpios = filtrar_outliers(grupo['precio'])
            if len(precios_limpios) > 0:
                sub = grupo.loc[precios_limpios.index].copy()
                df_filtrado.append(sub)

        if not df_filtrado:
            print(">>> Sin datos suficientes tras filtrar outliers")
            return None

        df = pd.concat(df_filtrado)
        print(f">>> Filas tras filtrar outliers y muestras pequeñas: {len(df)}")
        # ── FIN BLOQUE NUEVO ─────────────────────────────────────────────

        promedios = df.groupby(['sector', 'producto'])['precio'].mean()
        print(f">>> Promedios calculados:\n{promedios}")
        return promedios

    except Exception as e:
        print(f">>> Error en scraping: {e}")
        import traceback
        traceback.print_exc()
        return None


# ─────────────────────────────────────────────
# Guardar datos en la base de datos
# ─────────────────────────────────────────────
def actualizar_base_datos():
    hoy = datetime.now().date()
    existe = PrecioDiario.query.filter_by(fecha=hoy).first()

    if not existe:
        print(f"No hay datos de hoy ({hoy}). Obteniendo datos...")
        promedios = obtener_datos_osinergmin()

        if promedios is not None:
            for sector in SECTORES_MAP.keys():
                def precio(producto):
                    try:
                        return round(float(promedios.loc[(sector, producto)]), 2)
                    except KeyError:
                        return 0

                nuevo_registro = PrecioDiario(
                    fecha=hoy,
                    sector=sector,
                    g90=precio('GASOHOL 90'),
                    g95=precio('GASOHOL 95'),
                    g97=precio('GASOHOL 97'),
                    db5=precio('DB5 S-50'),
                    fuente='scraping'  # FIX: faltaba este campo
                )
                db.session.add(nuevo_registro)

            db.session.commit()
            print("✅ Datos guardados correctamente.")
        else:
            print("⚠️ No se pudieron obtener datos.")


# ─────────────────────────────────────────────
# Rutas de la aplicación
# ─────────────────────────────────────────────
@app.route('/')
def home():
    return render_template('index.html')


@app.route('/api/datos')
def api_datos():
    actualizar_base_datos()

    hace_6_meses = datetime.now() - timedelta(days=180)
    registros = PrecioDiario.query.filter(
        PrecioDiario.fecha >= hace_6_meses
    ).order_by(PrecioDiario.fecha.asc()).all()

    datos_formateados = []
    for reg in registros:
        datos_formateados.append({
            "fecha":   reg.fecha.strftime('%Y-%m-%d'),
            "sector":  reg.sector,
            "precios": {
                "g90": reg.g90,
                "g95": reg.g95,
                "g97": reg.g97,
                "db5": reg.db5
            }
        })
    return jsonify(datos_formateados)


# ─────────────────────────────────────────────
# SCHEDULER: Scraping automático diario
# ─────────────────────────────────────────────
# FIX: El scheduler debe iniciarse ANTES de app.run(),
# porque app.run() bloquea todo lo que viene después.
# Es como encender el despertador antes de dormirte,
# no después — si lo haces después, nunca suena.
def tarea_diaria():
    print("⏰ Scheduler: ejecutando scraping diario...")
    with app.app_context():
        actualizar_base_datos()

scheduler = BackgroundScheduler(timezone="America/Lima")
scheduler.add_job(
    func=tarea_diaria,
    trigger='cron',
    hour=8,
    minute=0,
    id='scraping_diario',
    replace_existing=True
)
scheduler.start()
print("✅ Scheduler iniciado — scraping diario a las 8:00am Lima")


# ─────────────────────────────────────────────
# Iniciar la aplicación (solo UNA vez, al final)
# ─────────────────────────────────────────────
with app.app_context():
    db.create_all()

if __name__ == '__main__':
    app.run(debug=True)