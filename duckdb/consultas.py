"""Consulta las tablas Delta de Silver y Gold desde DuckDB local, sin usar Databricks."""
from pathlib import Path

import duckdb

POST_HIST = "delta_scan('az://silver/stackoverflow/post_hist')"
USERS_HIST = "delta_scan('az://silver/stackoverflow/users_hist')"
GOLD = "delta_scan('az://gold/stackoverflow/cant_post_x_user_hist')"

CONSULTAS = {
    "Silver: posts por mes": f"""
        SELECT post_year AS anio, post_month AS mes, COUNT(*) AS filas, COUNT(DISTINCT id) AS ids,
               MIN(creation_date) AS primera_fecha, MAX(creation_date) AS ultima_fecha,
               MAX(fecha_cargue) AS fecha_cargue
        FROM {POST_HIST}
        GROUP BY 1, 2
        ORDER BY 1, 2
    """,
    "Silver: usuarios por mes": f"""
        SELECT creation_year AS anio, creation_month AS mes, COUNT(*) AS filas, COUNT(DISTINCT id) AS ids,
               MIN(creation_date) AS primera_fecha, MAX(creation_date) AS ultima_fecha,
               MAX(fecha_cargue) AS fecha_cargue
        FROM {USERS_HIST}
        GROUP BY 1, 2
        ORDER BY 1, 2
    """,
    "Gold: métricas por mes": f"""
        SELECT post_year AS anio, post_month AS mes, COUNT(*) AS filas, SUM(questions) AS preguntas,
               SUM(answers) AS respuestas, SUM(total_posts) AS posts, SUM(total_score) AS score,
               SUM(total_views) AS vistas, MAX(fecha_cargue) AS fecha_cargue
        FROM {GOLD}
        GROUP BY 1, 2
        ORDER BY 1, 2
    """,
    "Gold: usuarios con más posts": f"""
        SELECT user_id, post_month AS mes, display_name, questions, answers, total_posts, total_score, avg_score
        FROM {GOLD}
        ORDER BY total_posts DESC, user_id
        LIMIT 10
    """,
    "Gold replicado desde Silver: diferencias": f"""
        WITH replica AS (
            SELECT u.id AS user_id, p.post_year, p.post_month, COUNT(*) AS total_posts, SUM(p.score) AS total_score
            FROM {POST_HIST} p
            JOIN {USERS_HIST} u ON p.owner_user_id = u.id
            GROUP BY 1, 2, 3
        )
        SELECT COUNT(*) AS filas,
               COUNT(*) FILTER (g.total_posts IS DISTINCT FROM r.total_posts
                                OR g.total_score IS DISTINCT FROM r.total_score) AS diferencias
        FROM {GOLD} g
        FULL JOIN replica r USING (user_id, post_year, post_month)
    """,
}


def leer_cadena_conexion():
    archivo = Path(__file__).resolve().parent.parent / ".env"
    for linea in archivo.read_text().splitlines():
        if linea.startswith("AZURE_STORAGE_CONNECTION_STRING="):
            valor = linea.split("=", 1)[1].strip().strip('"')
            if valor:
                return valor
    raise SystemExit("Falta AZURE_STORAGE_CONNECTION_STRING en .env")


con = duckdb.connect()
# delta_scan interpreta las fechas de Delta en la zona de la sesión. En Bogotá quedarían corridas 5 horas.
con.execute("SET TimeZone = 'UTC'")
for extension in ("azure", "delta"):
    con.execute(f"INSTALL {extension}")
    con.execute(f"LOAD {extension}")

cadena = leer_cadena_conexion().replace("'", "''")
con.execute(f"CREATE SECRET adls (TYPE azure, CONNECTION_STRING '{cadena}')")

for titulo, consulta in CONSULTAS.items():
    print(f"\n{titulo}")
    print(con.execute(consulta).df().to_string(index=False))
