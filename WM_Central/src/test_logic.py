"""
    NO OLVIDAR: Este archivo es solo para pruebas. No se debe usar en producción.
"""

"""
    Prueba la lógica de Central SIn Kafkta ni sockets.
"""

import os


os.environ["WM_DB_PATH"] = "test_logic.db" # BD de pruebas
if os.path.exists("test_logic.db"):
    os.remove("test_logic.db")

import database as db
import logic