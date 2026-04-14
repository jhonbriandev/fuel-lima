import requests
from io import StringIO
import pandas as pd

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept-Language': 'es-PE,es;q=0.9',
}

urls_a_probar = [
    "https://www.preciocombustible.com/lima/?fecha=2025-10-01",
    "https://www.preciocombustible.com/lima/?date=2025-10-01",
    "https://www.preciocombustible.com/lima/2025-10-01/",
    "https://www.preciocombustible.com/lima/?d=2025-10-01",
    "https://www.preciocombustible.com/lima/",
]

for url in urls_a_probar:
    try:
        r = requests.get(url, headers=headers, timeout=15)
        print(f"Status {r.status_code} → {url}")
        tablas = pd.read_html(StringIO(r.text))
        if len(tablas) > 1:
            df = tablas[1]
            col = 'Fecha de Registro del Precio'
            if col in df.columns:
                fechas = df[col].dropna().unique()[:3]
                print(f"   → {len(df)} filas | fechas: {fechas}")
            else:
                print(f"   → {len(df)} filas | cols: {list(df.columns)[:4]}")
        else:
            print(f"   → Solo {len(tablas)} tabla(s)")
    except Exception as e:
        print(f"   ERROR: {e}")
    print()