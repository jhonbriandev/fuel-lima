"""
Script de importación única de datos históricos mensuales de Osinergmin.
Fuente: PDFs de Reportes Mensuales de Precios - Osinergmin
Ejecutar UNA SOLA VEZ: python importar_historico.py
"""

from datetime import date
from app import app, db, PrecioDiario

# ─────────────────────────────────────────────
# BLOQUE 1: Datos históricos extraídos de PDFs
# ─────────────────────────────────────────────
# Cada entrada tiene: (año, mes, g90, g95, db5)
# Fuente: Sección EESS/Grifos del reporte mensual de Osinergmin
# Columnas del PDF: G100|LL|DB5 S-50 UV|G98 BA|G97|G95|G90|G84|...
# G90 = col 7, G95 = col 6, DB5 = col 3

DATOS_HISTORICOS = [
    # (año,  mes, g90,   g95,   db5  )
    (2025,  10, 10.52, 10.26, 16.84),  # Octubre 2025
    (2025,  11, 10.89, 10.43, 16.50),  # Noviembre 2025
    (2025,  12, 10.93, 10.33, 16.47),  # Diciembre 2025
    (2026,   1, 11.27, 10.70, 16.43),  # Enero 2026
    (2026,   2, 11.97, 10.73, 16.42),  # Febrero 2026
    (2026,   3, 16.18, 15.12, 21.24),  # Marzo 2026
]

SECTORES = ['Lima Centro', 'Cono Norte', 'Cono Sur', 'Cono Este']

# ─────────────────────────────────────────────
# BLOQUE 2: Generar un registro por cada día
# del mes, para que el gráfico muestre una
# línea continua y no solo 6 puntos.
# ─────────────────────────────────────────────
# Piénsalo así: el precio de octubre aplica
# a TODOS los días de octubre, como un precio
# de lista que rige todo el mes.
import calendar

def dias_del_mes(anio, mes, g90, g95, db5):
    """Devuelve una lista de (fecha, g90, g95, db5) para cada día del mes"""
    _, ultimo_dia = calendar.monthrange(anio, mes)
    registros = []
    for dia in range(1, ultimo_dia + 1):
        registros.append((date(anio, mes, dia), g90, g95, db5))
    return registros

# ─────────────────────────────────────────────
# BLOQUE 3: Importar a la base de datos
# ─────────────────────────────────────────────
with app.app_context():
    db.create_all()

    importados = 0
    saltados   = 0

    for (anio, mes, g90, g95, db5) in DATOS_HISTORICOS:
        dias = dias_del_mes(anio, mes, g90, g95, db5)

        for (fecha_dia, p90, p95, p_db5) in dias:
            for sector in SECTORES:

                # Evitar duplicados si el script se corre más de una vez
                ya_existe = PrecioDiario.query.filter_by(
                    fecha=fecha_dia,
                    sector=sector,
                    fuente='pdf_historico'
                ).first()

                if ya_existe:
                    saltados += 1
                    continue

                nuevo = PrecioDiario(
                    fecha=fecha_dia,
                    sector=sector,
                    g90=p90,
                    g95=p95,
                    g97=None,     # No disponible en los PDFs
                    db5=p_db5,
                    fuente='pdf_historico'
                )
                db.session.add(nuevo)
                importados += 1

        # Guardamos al terminar cada mes completo
        db.session.commit()
        nombre_mes = date(anio, mes, 1).strftime('%B %Y')
        print(f"✅ {nombre_mes}: {len(dias) * len(SECTORES)} registros guardados")

    print(f"\n{'='*40}")
    print(f"Importación completa:")
    print(f"  Importados : {importados}")
    print(f"  Saltados   : {saltados} (ya existían)")
    print(f"  Total días : {importados // len(SECTORES)} días únicos")