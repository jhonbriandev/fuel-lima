# insertar_historico_directo.py
import os
import sys
import calendar
from datetime import date

# Verificamos que DATABASE_URL esté configurado
db_url = os.environ.get('DATABASE_URL')
os.environ['DATABASE_URL'] = "postgresql://fuel_db_y7ad_user:WWQ6ZhDP0VuxtJDjB7PGz2sPZhYxxEbq@dpg-d7es8m99rddc73ef26b0-a.oregon-postgres.render.com/fuel_db_y7ad"
db_url = os.environ.get('DATABASE_URL')
print(f"✅ Conectando a: {db_url[:40]}...")

from app import app, db, PrecioDiario

DATOS_HISTORICOS = [
    (2025, 10, 10.52, 10.26, 16.84),
    (2025, 11, 10.89, 10.43, 16.50),
    (2025, 12, 10.93, 10.33, 16.47),
    (2026,  1, 11.27, 10.70, 16.43),
    (2026,  2, 11.97, 10.73, 16.42),
    (2026,  3, 16.18, 15.12, 21.24),
]

SECTORES = ['Lima Centro', 'Cono Norte', 'Cono Sur', 'Cono Este']

with app.app_context():
    # Primero verificamos cuántos registros hay
    total_antes = PrecioDiario.query.count()
    print(f"📊 Registros ANTES: {total_antes}")

    # Borramos registros con fuente pdf_historico para reinsertar limpio
    borrados = PrecioDiario.query.filter_by(fuente='pdf_historico').delete()
    db.session.commit()
    print(f"🗑️  Registros históricos borrados: {borrados}")

    importados = 0
    for (anio, mes, g90, g95, db5) in DATOS_HISTORICOS:
        _, ultimo_dia = calendar.monthrange(anio, mes)
        for dia in range(1, ultimo_dia + 1):
            for sector in SECTORES:
                nuevo = PrecioDiario(
                    fecha=date(anio, mes, dia),
                    sector=sector,
                    g90=g90,
                    g95=g95,
                    g97=None,
                    db5=db5,
                    fuente='pdf_historico'
                )
                db.session.add(nuevo)
                importados += 1

        db.session.commit()
        print(f"✅ {anio}-{mes:02d}: insertado")

    total_despues = PrecioDiario.query.count()
    print(f"\n📊 Registros DESPUÉS: {total_despues}")
    print(f"✅ Importados: {importados}")