"""
Script simple para iniciar el servidor ETL
"""

import uvicorn
import sys
import os

# Agregar el directorio actual al path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

if __name__ == "__main__":
    print("🚀 Iniciando servidor ETL Avanzado...")
    print("📍 URL: http://localhost:8001")
    print("📚 Documentación: http://localhost:8001/docs")
    print("❤️ Health Check: http://localhost:8001/health")
    print("=" * 50)
    
    try:
        uvicorn.run(
            "main_advanced:app",
            host="0.0.0.0",
            port=8001,
            reload=True,
            log_level="info"
        )
    except KeyboardInterrupt:
        print("\n👋 Servidor detenido por el usuario")
    except Exception as e:
        print(f"❌ Error iniciando servidor: {e}")
        print("💡 Verifica que las dependencias estén instaladas:")
        print("   pip install -r requirements_simple.txt")
