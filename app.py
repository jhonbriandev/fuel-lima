import os
import requests
import pandas as pd
from flask import Flask, render_template, jsonify
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, timedelta

app = Flask(__name__)

# Configuración de Base de Datos (Render inyectará la URL real aquí)
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', 'sqlite:///local.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# Modelo de la Base de Datos
class PrecioDiario(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    fecha = db.Column(db.Date, unique=True, nullable=False)
    sector = db.Column(db.String(50), nullable=False)
    g90 = db.Column(db.Float)
    g95 = db.Column(db.Float)
    g97 = db.Column(db.Float)
    db5 = db.Column(db.Float)

# Mapeo de Distritos de OSINERGMIN a Sectores de Lima
SECTORES_MAP = {
    'Lima Centro': ['LIMA', 'JESUS MARIA', 'LA VICTORIA', 'BREÑA', 'LINCE', 'SAN MIGUEL', 'PUEBLO LIBRE', 'MAGDALENA', 'SAN ISIDRO', 'MIRAFLORES', 'SURQUILLO', 'SAN BORJA', 'SANTIAGO DE SURCO'],
    'Cono Norte': ['LOS OLIVOS', 'COMAS', 'INDEPENDENCIA', 'SAN MARTIN DE PORRES', 'PUENTE PIEDRA', 'ANCON', 'CARABAYLLO'],
    'Cono Sur': ['SAN JUAN DE MIRAFLORES', 'VILLA EL SALVADOR', 'VILLA MARIA DEL TRIUNFO', 'CHORRILLOS', 'LURIN', 'PACHACAMAC', 'SAN BARTOLO', 'PUNTA NEGRA', 'PUNTA HERMOSA'],
    'Cono Este': ['SAN JUAN DE LURIGANCHO', 'ATE', 'SANTA ANITA', 'EL AGUSTINO', 'RIMAC', 'SAN LUIS', 'CIENEGUILLA', 'CHACLACAYO', 'LA MOLINA']
}

PRODUCTOS_OBJETIVO = ['GASOHOL 90', 'GASOHOL 95', 'GASOHOL 97', 'DB5 S-50']

def obtener_datos_osinergmin():
    """Lee la tabla de precios desde preciocombustible.com"""
    
    url = "https://www.preciocombustible.com/lima/"
    
    # --- BLOQUE 1: Cabeceras ---
    # Le decimos al sitio que somos un navegador normal,
    # así evitamos que nos bloquee por ser un "robot"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept-Language': 'es-PE,es;q=0.9',
    }
    
    try:
        # --- BLOQUE 2: Descargar el HTML ---
        # requests.get descarga el contenido de la página,
        # como si tu navegador la abriera pero sin mostrarla
        print(">>> Conectando a preciocombustible.com...")
        response = requests.get(url, headers=headers, timeout=15)
        print(f">>> Status: {response.status_code}")
        
        # Si el status no es 200 (OK), lanzamos un error
        response.raise_for_status()
        
        # --- BLOQUE 3: Parsear el HTML ---
        # BeautifulSoup convierte el HTML crudo en algo
        # que podemos buscar fácilmente, como un buscador interno
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # --- BLOQUE 4: Encontrar la tabla ---
        # Buscamos todas las tablas en la página
        # pandas puede leer tablas HTML directamente con read_html()
        tablas = pd.read_html(response.text)
        print(f">>> Tablas encontradas: {len(tablas)}")
        
        if not tablas:
            print(">>> No se encontraron tablas")
            return None
        
        # La primera tabla suele ser la principal
        # Si no funciona, prueba con tablas[1], tablas[2], etc.
        df = tablas[0]
        print(f">>> Columnas: {df.columns.tolist()}")
        print(f">>> Primeras filas:\n{df.head()}")
        
        return df  # Por ahora retornamos el df crudo para ver su estructura
        
    except Exception as e:
        print(f">>> Error en scraping: {e}")
        return None
    
def actualizar_base_datos():
    """Verifica si hay datos de hoy, si no, hace scraping y guarda"""
    hoy = datetime.now().date()
    existe = PrecioDiario.query.filter_by(fecha=hoy).first()
    
    if not existe:
        print(f"No hay datos de hoy ({hoy}). Obteniendo de Osinergmin...")
        promedios = obtener_datos_osinergmin()
        
        if promedios is not None:
            for sector in SECTORES_MAP.keys():
                if sector in promedios.index:
                    nuevo_registro = PrecioDiario(
                        fecha=hoy,
                        sector=sector,
                        g90=float(promedios.loc[sector, 'GASOHOL 90']) if 'GASOHOL 90' in promedios.columns else 0,
                        g95=float(promedios.loc[sector, 'GASOHOL 95']) if 'GASOHOL 95' in promedios.columns else 0,
                        g97=float(promedios.loc[sector, 'GASOHOL 97']) if 'GASOHOL 97' in promedios.columns else 0,
                        db5=float(promedios.loc[sector, 'DB5 S-50']) if 'DB5 S-50' in promedios.columns else 0
                    )
                    db.session.add(nuevo_registro)
            db.session.commit()
            print("Datos guardados correctamente.")

# Rutas de la aplicación
@app.route('/')
def home():
    return render_template('index.html')

@app.route('/api/datos')
def api_datos():
    actualizar_base_datos() # Se ejecuta cada vez que se piden datos para asegurar que estén actualizados
    
    # Obtener los últimos 6 meses de historial
    hace_6_meses = datetime.now() - timedelta(days=180)
    registros = PrecioDiario.query.filter(PrecioDiario.fecha >= hace_6_meses).order_by(PrecioDiario.fecha.asc()).all()
    
    # Formatear para el frontend
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

# Crear BD al iniciar
with app.app_context():
    db.create_all()

if __name__ == '__main__':
    app.run(debug=True)