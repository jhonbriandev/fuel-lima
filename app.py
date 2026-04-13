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
    """Se conecta a la API interna de Osinergmin y extrae los datos"""
    url = "https://appserver.osinergmin.gob.pe/preciosinferior/api/PrecioInferior/ObtenerPrecioInferior"
    payload = {
        "idDistrito": 0, "idProducto": 0, "idProvincia": 0, "idDepartamento": 15,
        "codSector": 0, "codVia": 0, "nomDistrito": "", "nomProducto": "",
        "nomProvincia": "", "nomDepartamento": "LIMA"
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=15)
        data = response.json()
        df = pd.DataFrame(data)
        
        # Filtrar solo los productos que nos interesan y quitar outliers (precios basura)
        df = df[df['nomProducto'].isin(PRODUCTOS_OBJETIVO)]
        df = df[pd.to_numeric(df['precio'], errors='coerce') > 10]  # Ignorar precios menores a 10 soles        
        # Asignar Sector basado en el distrito
        def asignar_sector(distrito):
            distrito_upper = distrito.upper()
            for sector, distritos in SECTORES_MAP.items():
                if distrito_upper in distritos:
                    return sector
            return None
            
        df['sector'] = df['nomDistrito'].apply(asignar_sector)
        df = df.dropna(subset=['sector']) # Eliminar distritos que no mapeamos
        
        # Agrupar por Sector y Producto, calculando el promedio real
        resultado = df.groupby(['sector', 'nomProducto'])['precio'].mean().unstack(fill_value=0)
        
        return resultado
    except Exception as e:
        print(f"Error en scraping: {e}")
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