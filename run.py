#!/usr/bin/env python3
"""
Script para ejecutar la aplicación ETL Tool
"""

import uvicorn
from config import APP_CONFIG

if __name__ == "__main__":
    print("🚀 Iniciando ETL Tool API...")
    print(f"📡 Servidor disponible en: http://localhost:{APP_CONFIG['port']}")
    print("📋 Endpoints disponibles:")
    print("   - GET  /health - Estado de la aplicación")
    print("   - POST /api/etl/upload - Cargar archivo")
    print("   - POST /api/etl/preview - Vista previa de datos")
    print("   - GET  /api/etl/tables - Obtener tablas disponibles")
    print("   - GET  /api/etl/columns/{table} - Obtener columnas de tabla")
    print("   - POST /api/etl/process - Procesar y cargar datos")
    print("   - POST /api/etl/config/save - Guardar configuración")
    print("   - GET  /api/etl/config/{name} - Obtener configuración")
    print("=" * 60)
    
    uvicorn.run(
        "main:app",
        host=APP_CONFIG["host"],
        port=APP_CONFIG["port"],
        reload=APP_CONFIG["reload"],
        log_level="info"
    )
