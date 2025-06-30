"""
Script de configuración e instalación para el Sistema ETL Avanzado
"""

import subprocess
import sys
import os
from pathlib import Path
import psycopg2

def run_command(command, description):
    """Ejecutar comando y mostrar resultado"""
    print(f"🔧 {description}...")
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ {description} completado")
            if result.stdout:
                print(f"   Output: {result.stdout.strip()}")
            return True
        else:
            print(f"❌ Error en {description}")
            print(f"   Error: {result.stderr.strip()}")
            return False
    except Exception as e:
        print(f"❌ Excepción en {description}: {e}")
        return False

def check_python_version():
    """Verificar versión de Python"""
    print("🐍 Verificando versión de Python...")
    version = sys.version_info
    if version.major >= 3 and version.minor >= 8:
        print(f"✅ Python {version.major}.{version.minor}.{version.micro} - OK")
        return True
    else:
        print(f"❌ Python {version.major}.{version.minor}.{version.micro} - Se requiere Python 3.8+")
        return False

def check_database_connection():
    """Verificar conexión a base de datos"""
    print("🗄️ Verificando conexión a base de datos...")
    try:
        from config import DATABASE_CONFIG
        conn = psycopg2.connect(**DATABASE_CONFIG)
        conn.close()
        print("✅ Conexión a base de datos - OK")
        return True
    except Exception as e:
        print(f"❌ Error conectando a base de datos: {e}")
        print("💡 Verifica la configuración en config.py")
        return False

def install_dependencies():
    """Instalar dependencias"""
    print("📦 Instalando dependencias...")
    
    # Verificar si requirements.txt existe
    if not Path("requirements.txt").exists():
        print("❌ Archivo requirements.txt no encontrado")
        return False
    
    # Instalar dependencias
    return run_command(
        f"{sys.executable} -m pip install -r requirements.txt",
        "Instalación de dependencias"
    )

def create_database_tables():
    """Crear tablas de base de datos"""
    print("🏗️ Creando tablas de base de datos...")
    
    # Verificar si el archivo de migración existe
    migration_file = Path("migrations/create_etl_advanced_tables.sql")
    if not migration_file.exists():
        print("❌ Archivo de migración no encontrado")
        return False
    
    try:
        from config import DATABASE_CONFIG
        conn = psycopg2.connect(**DATABASE_CONFIG)
        cursor = conn.cursor()
        
        # Leer y ejecutar el archivo SQL
        with open(migration_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        cursor.execute(sql_content)
        conn.commit()
        cursor.close()
        conn.close()
        
        print("✅ Tablas creadas exitosamente")
        return True
        
    except Exception as e:
        print(f"❌ Error creando tablas: {e}")
        return False

def create_directories():
    """Crear directorios necesarios"""
    print("📁 Creando directorios necesarios...")
    
    directories = [
        "temp_uploads",
        "logs",
        "tests/test_data"
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"   ✅ {directory}")
    
    return True

def test_basic_functionality():
    """Probar funcionalidad básica"""
    print("🧪 Probando funcionalidad básica...")
    
    try:
        # Importar módulos principales
        from models.base import BaseModel
        from modules.validator import DataValidator
        from modules.transformer import DataTransformer
        from modules.processor import DataProcessor
        from modules.notifier import NotificationManager
        from modules.job_manager import JobManager
        
        print("✅ Todos los módulos se importaron correctamente")
        return True
        
    except Exception as e:
        print(f"❌ Error importando módulos: {e}")
        return False

def create_sample_data():
    """Crear datos de muestra para pruebas"""
    print("📊 Creando datos de muestra...")
    
    test_data_dir = Path("tests/test_data")
    test_data_dir.mkdir(parents=True, exist_ok=True)
    
    # Crear archivo CSV de prueba
    sample_csv = test_data_dir / "sample_data.csv"
    with open(sample_csv, 'w', encoding='utf-8') as f:
        f.write("nombre,edad,email,salario,fecha_ingreso\n")
        f.write("Juan Pérez,30,juan@empresa.com,50000,2023-01-15\n")
        f.write("María García,25,maria@empresa.com,45000,2023-02-20\n")
        f.write("Carlos López,35,,60000,2023-03-10\n")  # Email vacío
        f.write("Ana Martín,28,ana@invalid,55000,2023-04-05\n")  # Email inválido
        f.write("Luis Rodríguez,32,luis@empresa.com,52000,2023-05-12\n")
    
    print(f"✅ Archivo de muestra creado: {sample_csv}")
    return True

def show_next_steps():
    """Mostrar próximos pasos"""
    print("\n🎉 ¡INSTALACIÓN COMPLETADA!")
    print("=" * 50)
    print("📋 PRÓXIMOS PASOS:")
    print()
    print("1. Iniciar el servidor ETL:")
    print("   python main_advanced.py")
    print()
    print("2. Probar la API:")
    print("   python debug_guide.py")
    print()
    print("3. Ejecutar tests:")
    print("   python -m pytest tests/")
    print()
    print("4. Acceder a la documentación:")
    print("   http://localhost:8001/docs")
    print()
    print("5. Probar endpoints básicos:")
    print("   curl http://localhost:8001/health")
    print()
    print("📚 RECURSOS:")
    print("- README.md: Documentación completa")
    print("- debug_guide.py: Herramienta de debug interactiva")
    print("- tests/: Suite de pruebas automatizadas")
    print()

def main():
    """Función principal de configuración"""
    print("🚀 CONFIGURACIÓN DEL SISTEMA ETL AVANZADO")
    print("=" * 50)
    print("Este script configurará automáticamente el sistema ETL")
    print("=" * 50)
    
    steps = [
        ("Verificar Python", check_python_version),
        ("Verificar base de datos", check_database_connection),
        ("Crear directorios", create_directories),
        ("Instalar dependencias", install_dependencies),
        ("Crear tablas", create_database_tables),
        ("Probar módulos", test_basic_functionality),
        ("Crear datos de muestra", create_sample_data)
    ]
    
    success_count = 0
    total_steps = len(steps)
    
    for step_name, step_function in steps:
        print(f"\n📋 PASO: {step_name}")
        print("-" * 30)
        
        if step_function():
            success_count += 1
        else:
            print(f"\n⚠️ Error en paso: {step_name}")
            response = input("¿Continuar con el siguiente paso? (s/n): ").lower()
            if response != 's':
                break
    
    print(f"\n📊 RESUMEN:")
    print(f"✅ Pasos completados: {success_count}/{total_steps}")
    print(f"📈 Tasa de éxito: {(success_count/total_steps)*100:.1f}%")
    
    if success_count == total_steps:
        show_next_steps()
    else:
        print("\n⚠️ CONFIGURACIÓN INCOMPLETA")
        print("Revisa los errores anteriores y ejecuta el script nuevamente")
        print("O ejecuta los pasos manualmente según la documentación")

if __name__ == "__main__":
    main()
