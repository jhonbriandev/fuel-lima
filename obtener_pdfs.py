import requests
from io import BytesIO
import pdfplumber
import re

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}

meses_urls = [
    ("Octubre 2025",   "https://www.osinergmin.gob.pe/seccion/centro_documental/hidrocarburos/SCOP/SCOP-DOCS/2025/Reporte-Mensual-Precios-Octubre-2025.pdf"),
    ("Noviembre 2025", "https://www.osinergmin.gob.pe/seccion/centro_documental/hidrocarburos/SCOP/SCOP-DOCS/2025/Reporte-Mensual-Precios-Noviembre-2025.pdf"),
    ("Diciembre 2025", "https://www.osinergmin.gob.pe/seccion/centro_documental/hidrocarburos/SCOP/SCOP-DOCS/2025/Reporte-Mensual-Precios-Diciembre-2025.pdf"),
    ("Enero 2026",     "https://www.osinergmin.gob.pe/seccion/centro_documental/hidrocarburos/SCOP/SCOP-DOCS/2026/Reporte-Mensual-Precios-Enero-2026.pdf"),
    ("Febrero 2026",   "https://www.osinergmin.gob.pe/seccion/centro_documental/hidrocarburos/SCOP/SCOP-DOCS/2026/Reporte-Mensual-Precios-Febrero-2026.pdf"),
    ("Marzo 2026",     "https://www.osinergmin.gob.pe/seccion/centro_documental/hidrocarburos/SCOP/SCOP-DOCS/2026/Reporte-Mensual-Precios-Marzo-2026.pdf"),
]

for nombre, url in meses_urls:
    try:
        r = requests.get(url, headers=headers, timeout=20)
        if r.status_code != 200:
            print(f"❌ {nombre}: status {r.status_code}")
            continue

        # Leer el PDF desde memoria (sin guardarlo en disco)
        with pdfplumber.open(BytesIO(r.content)) as pdf:
            texto = ""
            for pagina in pdf.pages:
                texto += pagina.extract_text() or ""

        # Buscar la fila de LIMA en la sección de EESS/Grifos
        lineas = texto.split('\n')
        for i, linea in enumerate(lineas):
            if 'LIMA' in linea and any(c.isdigit() for c in linea):
                print(f"✅ {nombre}: {linea.strip()}")
                break
        else:
            print(f"⚠️  {nombre}: fila LIMA no encontrada")
            # Mostrar contexto para debug
            for linea in lineas:
                if 'LIMA' in linea:
                    print(f"   Encontré: {linea.strip()}")

    except Exception as e:
        print(f"❌ {nombre}: {e}")