import os
import sqlite3
from contextlib import contextmanager

# Ruta de la BD. En local usa un fichero junto al código;
# más adelante podrá cambiarse con una variable de entorno (Railway).
DB_PATH = os.environ.get("WM_DB_PATH", "wm_central.db")

# Estados que ve el panel
AVAILABLE = "DISPONIBLE"
WATERING = "REGANDO"
LEAK = "FUGA"
OUT_OF_SERVICE = "FUERA_DE_SERVICIO"
DISCONNECTED = "DESCONECTADA"


@contextmanager
def _conn():
    """Una conexión corta por operación (seguro con varios hilos)."""
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Crea las tablas si no existen y deja todas las WS como desconectadas.

    Se llama al arrancar CENTRAL: hasta que una WS conecte, no se
    conoce su estado real (requisito del enunciado).
    """
    with _conn() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS watering_station (
                id          TEXT PRIMARY KEY,
                location    TEXT NOT NULL,
                connected   INTEGER NOT NULL DEFAULT 0,
                blocked     INTEGER NOT NULL DEFAULT 0,
                leak        INTEGER NOT NULL DEFAULT 0,
                watering    INTEGER NOT NULL DEFAULT 0,
                flow        REAL    NOT NULL DEFAULT 0,
                volume      REAL    NOT NULL DEFAULT 0,
                operator_id TEXT,
                last_seen   TEXT
            )""")
        c.execute("""
            CREATE TABLE IF NOT EXISTS operator (
                id   TEXT PRIMARY KEY,
                name TEXT
            )""")
        c.execute("""
            UPDATE watering_station
            SET connected = 0, watering = 0, flow = 0, operator_id = NULL
        """)


# ---------------------------------------------------------------- operarios
def add_operator(operator_id, name=None):
    with _conn() as c:
        c.execute("INSERT OR IGNORE INTO operator (id, name) VALUES (?, ?)",
                  (operator_id, name))


def operator_exists(operator_id) -> bool:
    with _conn() as c:
        row = c.execute("SELECT 1 FROM operator WHERE id = ?",
                        (operator_id,)).fetchone()
    return row is not None


# ----------------------------------------------------------------- estaciones
def _derive_status(row) -> str:
    if not row["connected"]:
        return DISCONNECTED
    if row["blocked"]:
        return OUT_OF_SERVICE
    if row["leak"]:
        return LEAK
    if row["watering"]:
        return WATERING
    return AVAILABLE


def register_station(ws_id, location):
    """Alta o reconexión de una WS (mensaje REGISTER del Monitor).

    Si ya existía conserva su 'blocked' (orden de CENTRAL) y su ubicación
    se actualiza. La marca como conectada y sin riego en curso.
    """
    
    with _conn() as c:
        c.execute("""
            INSERT INTO watering_station (id, location, connected, last_seen)
            VALUES (?, ?, 1, datetime('now'))
            ON CONFLICT(id) DO UPDATE SET
                location  = excluded.location,
                connected = 1,
                leak      = 0,
                watering  = 0,
                flow      = 0,
                operator_id = NULL,
                last_seen = datetime('now')
        """, (ws_id, location))


def touch_station(ws_id):
    """Anota que hemos recibido un STATUS reciente (latido)."""
    with _conn() as c:
        c.execute("UPDATE watering_station SET last_seen = datetime('now') "
                  "WHERE id = ?", (ws_id,))


def set_disconnected(ws_id):
    """El socket se cerró o dejaron de llegar latidos."""
    with _conn() as c:
        c.execute("""UPDATE watering_station
                     SET connected = 0, watering = 0, flow = 0,
                         operator_id = NULL
                     WHERE id = ?""", (ws_id,))


def set_leak(ws_id, leak: bool) -> bool:
    """Marca o desmarca fuga. Devuelve True si había un riego en curso
    que ha quedado cortado (para que CENTRAL mande STOP y avise)."""
    with _conn() as c:
        row = c.execute("SELECT watering FROM watering_station WHERE id = ?",
                        (ws_id,)).fetchone()
        was_watering = bool(row and row["watering"])
        if leak:
            c.execute("""UPDATE watering_station
                         SET leak = 1, watering = 0, flow = 0,
                             operator_id = NULL
                         WHERE id = ?""", (ws_id,))
        else:
            c.execute("UPDATE watering_station SET leak = 0 WHERE id = ?",
                      (ws_id,))
    return leak and was_watering


def set_blocked(ws_id, blocked: bool) -> bool:
    """Bloquea (fuera de servicio) o reactiva. Igual que set_leak,
    devuelve True si se cortó un riego en curso."""
    with _conn() as c:
        row = c.execute("SELECT watering FROM watering_station WHERE id = ?",
                        (ws_id,)).fetchone()
        was_watering = bool(row and row["watering"])
        if blocked:
            c.execute("""UPDATE watering_station
                         SET blocked = 1, watering = 0, flow = 0,
                             operator_id = NULL
                         WHERE id = ?""", (ws_id,))
        else:
            c.execute("UPDATE watering_station SET blocked = 0 WHERE id = ?",
                      (ws_id,))
    return blocked and was_watering


def can_water(ws_id):
    """Devuelve (True, '') o (False, motivo). Sirve para los mensajes
    NOTIFY/DENIED al operario."""
    st = get_station(ws_id)
    if st is None:
        return False, "La estación no existe"
    if st["status"] == AVAILABLE:
        return True, ""
    motivos = {
        DISCONNECTED: "La estación está desconectada",
        OUT_OF_SERVICE: "La estación está fuera de servicio",
        LEAK: "La estación tiene una fuga",
        WATERING: "La estación ya está regando",
    }
    return False, motivos[st["status"]]


def start_watering(ws_id, operator_id) -> bool:
    """Intenta pasar la WS a REGANDO de forma atómica.

    El UPDATE solo se aplica si la WS sigue disponible en ese instante,
    así dos operarios pidiendo a la vez la misma WS no la reservan los dos.
    """
    with _conn() as c:
        cur = c.execute("""
            UPDATE watering_station
            SET watering = 1, flow = 0, volume = 0, operator_id = ?
            WHERE id = ? AND connected = 1 AND blocked = 0
                  AND leak = 0 AND watering = 0
        """, (operator_id, ws_id))
        return cur.rowcount == 1


def update_telemetry(ws_id, flow, volume):
    with _conn() as c:
        c.execute("UPDATE watering_station SET flow = ?, volume = ? "
                  "WHERE id = ?", (flow, volume, ws_id))


def end_watering(ws_id):
    with _conn() as c:
        c.execute("""UPDATE watering_station
                     SET watering = 0, flow = 0, operator_id = NULL
                     WHERE id = ?""", (ws_id,))


def get_station(ws_id):
    with _conn() as c:
        row = c.execute("SELECT * FROM watering_station WHERE id = ?",
                        (ws_id,)).fetchone()
    if row is None:
        return None
    d = dict(row)
    d["status"] = _derive_status(row)
    return d


def list_stations():
    with _conn() as c:
        rows = c.execute(
            "SELECT * FROM watering_station ORDER BY id").fetchall()
    result = []
    for row in rows:
        d = dict(row)
        d["status"] = _derive_status(row)
        result.append(d)
    return result