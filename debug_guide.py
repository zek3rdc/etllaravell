"""
Guía de Debug y Testing Manual para Sistema ETL Avanzado
"""

import requests
import json
import time
from pathlib import Path

# Configuración
BASE_URL = "http://localhost:8001"  # Cambiar puerto si es necesario
API_URL = f"{BASE_URL}/api/etl"

class ETLDebugger:
    """Clase para hacer debug del sistema ETL"""
    
    def __init__(self):
        self.session_id = None
        self.job_id = None
        
    def print_response(self, response, title="Response"):
        """Imprimir respuesta de forma legible"""
        print(f"\n{'='*50}")
        print(f"{title}")
        print(f"{'='*50}")
        print(f"Status Code: {response.status_code}")
        print(f"Headers: {dict(response.headers)}")
        
        try:
            data = response.json()
            print(f"JSON Response:")
            print(json.dumps(data, indent=2, ensure_ascii=False))
        except:
            print(f"Text Response: {response.text}")
        
        print(f"{'='*50}\n")
    
    def test_health(self):
        """1. Probar endpoint de salud"""
        print("🔍 PASO 1: Probando endpoint de salud...")
        
        try:
            response = requests.get(f"{API_URL}/health")
            self.print_response(response, "Health Check")
            
            if response.status_code == 200:
                print("✅ Servidor funcionando correctamente")
                return True
            else:
                print("❌ Servidor no responde correctamente")
                return False
                
        except requests.exceptions.ConnectionError:
            print("❌ Error: No se puede conectar al servidor")
            print("💡 Solución: Asegúrate de que el servidor esté ejecutándose:")
            print("   cd etl_app && python main_advanced.py")
            return False
        except Exception as e:
            print(f"❌ Error inesperado: {e}")
            return False
    
    def test_upload(self):
        """2. Probar carga de archivo"""
        print("🔍 PASO 2: Probando carga de archivo...")
        
        # Crear archivo de prueba
        test_file = Path("test_data.csv")
        with open(test_file, 'w', encoding='utf-8') as f:
            f.write("nombre,edad,email,salario\n")
            f.write("Juan Pérez,30,juan@test.com,50000\n")
            f.write("María García,25,maria@test.com,45000\n")
            f.write("Carlos López,35,,60000\n")  # Email vacío para probar validaciones
            f.write("Ana Martín,28,ana@invalid,55000\n")  # Email inválido
        
        try:
            with open(test_file, 'rb') as f:
                files = {'file': ('test_data.csv', f, 'text/csv')}
                response = requests.post(f"{API_URL}/upload", files=files)
            
            self.print_response(response, "File Upload")
            
            if response.status_code == 200:
                data = response.json()
                self.session_id = data.get('session_id')
                print(f"✅ Archivo cargado exitosamente")
                print(f"📝 Session ID: {self.session_id}")
                return True
            else:
                print("❌ Error cargando archivo")
                return False
                
        except Exception as e:
            print(f"❌ Error: {e}")
            return False
        finally:
            # Limpiar archivo de prueba
            if test_file.exists():
                test_file.unlink()
    
    def test_preview(self):
        """3. Probar vista previa"""
        print("🔍 PASO 3: Probando vista previa de datos...")
        
        if not self.session_id:
            print("❌ Error: No hay session_id disponible")
            return False
        
        try:
            data = {
                'session_id': self.session_id,
                'sheet': 'default'
            }
            response = requests.post(f"{API_URL}/preview", data=data)
            self.print_response(response, "Data Preview")
            
            if response.status_code == 200:
                print("✅ Vista previa obtenida exitosamente")
                return True
            else:
                print("❌ Error obteniendo vista previa")
                return False
                
        except Exception as e:
            print(f"❌ Error: {e}")
            return False
    
    def test_validation(self):
        """4. Probar validaciones"""
        print("🔍 PASO 4: Probando validaciones de datos...")
        
        if not self.session_id:
            print("❌ Error: No hay session_id disponible")
            return False
        
        try:
            payload = {
                'session_id': self.session_id,
                'sheet': 'default'
            }
            response = requests.post(f"{API_URL}/validate", json=payload)
            self.print_response(response, "Data Validation")
            
            if response.status_code == 200:
                data = response.json()
                print("✅ Validaciones completadas")
                print(f"📊 Resumen:")
                print(f"   - Errores: {data['summary']['errors']}")
                print(f"   - Advertencias: {data['summary']['warnings']}")
                print(f"   - Info: {data['summary']['info']}")
                return True
            else:
                print("❌ Error en validaciones")
                return False
                
        except Exception as e:
            print(f"❌ Error: {e}")
            return False
    
    def test_transformations(self):
        """5. Probar transformaciones disponibles"""
        print("🔍 PASO 5: Probando transformaciones disponibles...")
        
        try:
            response = requests.get(f"{API_URL}/transformations/available")
            self.print_response(response, "Available Transformations")
            
            if response.status_code == 200:
                print("✅ Transformaciones obtenidas exitosamente")
                return True
            else:
                print("❌ Error obteniendo transformaciones")
                return False
                
        except Exception as e:
            print(f"❌ Error: {e}")
            return False
    
    def test_custom_transformation(self):
        """6. Probar transformación personalizada"""
        print("🔍 PASO 6: Probando creación de transformación personalizada...")
        
        code = """
def transform(value):
    '''Convierte texto a mayúsculas y remueve espacios extra'''
    if isinstance(value, str):
        return value.upper().strip()
    return value
"""
        
        try:
            data = {
                'name': 'clean_uppercase',
                'description': 'Limpia y convierte a mayúsculas',
                'code': code,
                'parameters': '{}'
            }
            response = requests.post(f"{API_URL}/transformations/custom", data=data)
            self.print_response(response, "Custom Transformation")
            
            if response.status_code == 200:
                print("✅ Transformación personalizada creada")
                return True
            else:
                print("❌ Error creando transformación personalizada")
                return False
                
        except Exception as e:
            print(f"❌ Error: {e}")
            return False
    
    def test_async_processing(self):
        """7. Probar procesamiento asíncrono"""
        print("🔍 PASO 7: Probando procesamiento asíncrono...")
        
        if not self.session_id:
            print("❌ Error: No hay session_id disponible")
            return False
        
        try:
            data = {
                'session_id': self.session_id,
                'sheet': 'default',
                'column_mapping': json.dumps({
                    'nombre': 'name',
                    'edad': 'age',
                    'email': 'email',
                    'salario': 'salary'
                }),
                'transformations': json.dumps({
                    'name': {
                        'type': 'text',
                        'options': {'text_transform': 'title'}
                    },
                    'email': {
                        'type': 'text',
                        'options': {'text_transform': 'lower'}
                    }
                }),
                'target_table': 'test_personas',
                'mode': 'insert',
                'config_name': 'test_config_debug'
            }
            
            response = requests.post(f"{API_URL}/process-async", data=data)
            self.print_response(response, "Async Processing")
            
            if response.status_code == 200:
                result = response.json()
                self.job_id = result.get('job_id')
                print(f"✅ Procesamiento iniciado")
                print(f"📝 Job ID: {self.job_id}")
                return True
            else:
                print("❌ Error iniciando procesamiento")
                return False
                
        except Exception as e:
            print(f"❌ Error: {e}")
            return False
    
    def test_job_status(self):
        """8. Probar estado de trabajo"""
        print("🔍 PASO 8: Probando estado de trabajo...")
        
        if not self.job_id:
            print("❌ Error: No hay job_id disponible")
            return False
        
        try:
            response = requests.get(f"{API_URL}/job-status/{self.job_id}")
            self.print_response(response, "Job Status")
            
            if response.status_code == 200:
                data = response.json()
                status = data.get('status', 'unknown')
                print(f"✅ Estado del trabajo: {status}")
                
                # Monitorear progreso
                if status in ['pending', 'processing']:
                    print("⏳ Monitoreando progreso...")
                    for i in range(10):  # Máximo 10 intentos
                        time.sleep(2)
                        response = requests.get(f"{API_URL}/job-status/{self.job_id}")
                        if response.status_code == 200:
                            data = response.json()
                            new_status = data.get('status', 'unknown')
                            progress = data.get('progress', 0)
                            print(f"   Estado: {new_status}, Progreso: {progress}%")
                            
                            if new_status in ['completed', 'failed']:
                                break
                
                return True
            else:
                print("❌ Error obteniendo estado del trabajo")
                return False
                
        except Exception as e:
            print(f"❌ Error: {e}")
            return False
    
    def test_config_management(self):
        """9. Probar gestión de configuraciones"""
        print("🔍 PASO 9: Probando gestión de configuraciones...")
        
        config = {
            "name": "debug_config",
            "description": "Configuración de debug",
            "column_mapping": {
                "nombre": "name",
                "edad": "age",
                "email": "email"
            },
            "transformations": {
                "name": {
                    "type": "text",
                    "options": {"text_transform": "title"}
                }
            },
            "target_table": "test_table",
            "mode": "insert",
            "version": "1.0"
        }
        
        try:
            # Guardar configuración
            response = requests.post(f"{API_URL}/config/save-versioned", json=config)
            self.print_response(response, "Save Config")
            
            if response.status_code == 200:
                print("✅ Configuración guardada")
                
                # Obtener versiones
                response = requests.get(f"{API_URL}/config/debug_config/versions")
                self.print_response(response, "Config Versions")
                
                return True
            else:
                print("❌ Error guardando configuración")
                return False
                
        except Exception as e:
            print(f"❌ Error: {e}")
            return False
    
    def test_history_and_stats(self):
        """10. Probar historial y estadísticas"""
        print("🔍 PASO 10: Probando historial y estadísticas...")
        
        try:
            # Historial
            response = requests.get(f"{API_URL}/history")
            self.print_response(response, "Load History")
            
            # Estadísticas
            response = requests.get(f"{API_URL}/history/statistics")
            self.print_response(response, "Load Statistics")
            
            print("✅ Historial y estadísticas obtenidos")
            return True
            
        except Exception as e:
            print(f"❌ Error: {e}")
            return False
    
    def test_notifications(self):
        """11. Probar notificaciones"""
        print("🔍 PASO 11: Probando sistema de notificaciones...")
        
        try:
            # Plantillas
            response = requests.get(f"{API_URL}/notifications/templates")
            self.print_response(response, "Notification Templates")
            
            # Configurar notificación de prueba
            config = {
                "name": "debug_notification",
                "type": "email",
                "config": {
                    "smtp_server": "smtp.test.com",
                    "smtp_port": 587,
                    "username": "test@test.com",
                    "password": "test123",
                    "from_email": "etl@test.com",
                    "to_emails": ["admin@test.com"]
                },
                "events": {
                    "types": ["load_completed", "load_failed"],
                    "conditions": {"only_on_errors": False}
                }
            }
            
            response = requests.post(f"{API_URL}/notifications/config", json=config)
            self.print_response(response, "Create Notification Config")
            
            print("✅ Sistema de notificaciones probado")
            return True
            
        except Exception as e:
            print(f"❌ Error: {e}")
            return False
    
    def run_full_test(self):
        """Ejecutar todas las pruebas"""
        print("🚀 INICIANDO TESTING COMPLETO DEL SISTEMA ETL AVANZADO")
        print("=" * 60)
        
        tests = [
            self.test_health,
            self.test_upload,
            self.test_preview,
            self.test_validation,
            self.test_transformations,
            self.test_custom_transformation,
            self.test_async_processing,
            self.test_job_status,
            self.test_config_management,
            self.test_history_and_stats,
            self.test_notifications
        ]
        
        passed = 0
        failed = 0
        
        for test in tests:
            try:
                if test():
                    passed += 1
                else:
                    failed += 1
            except Exception as e:
                print(f"❌ Error ejecutando {test.__name__}: {e}")
                failed += 1
            
            print("\n" + "-" * 60 + "\n")
        
        print("📊 RESUMEN DE PRUEBAS:")
        print(f"✅ Pasaron: {passed}")
        print(f"❌ Fallaron: {failed}")
        print(f"📈 Tasa de éxito: {(passed/(passed+failed)*100):.1f}%")
        
        if failed > 0:
            print("\n🔧 CONSEJOS DE DEBUG:")
            print("1. Verificar que el servidor esté ejecutándose")
            print("2. Verificar configuración de base de datos")
            print("3. Revisar logs del servidor para errores específicos")
            print("4. Verificar que las tablas ETL estén creadas")

def main():
    """Función principal"""
    print("🎯 GUÍA DE DEBUG Y TESTING - SISTEMA ETL AVANZADO")
    print("=" * 60)
    print("Esta herramienta te ayudará a probar y hacer debug del sistema ETL")
    print("Asegúrate de que el servidor esté ejecutándose en http://localhost:8001")
    print("=" * 60)
    
    debugger = ETLDebugger()
    
    while True:
        print("\n🔧 OPCIONES DE DEBUG:")
        print("1. Ejecutar todas las pruebas")
        print("2. Probar endpoint específico")
        print("3. Mostrar guía de debug")
        print("0. Salir")
        
        choice = input("\nSelecciona una opción: ").strip()
        
        if choice == "1":
            debugger.run_full_test()
        elif choice == "2":
            print("\n📋 ENDPOINTS DISPONIBLES:")
            tests = [
                ("Health Check", debugger.test_health),
                ("Upload File", debugger.test_upload),
                ("Preview Data", debugger.test_preview),
                ("Validate Data", debugger.test_validation),
                ("Transformations", debugger.test_transformations),
                ("Custom Transformation", debugger.test_custom_transformation),
                ("Async Processing", debugger.test_async_processing),
                ("Job Status", debugger.test_job_status),
                ("Config Management", debugger.test_config_management),
                ("History & Stats", debugger.test_history_and_stats),
                ("Notifications", debugger.test_notifications)
            ]
            
            for i, (name, _) in enumerate(tests, 1):
                print(f"{i}. {name}")
            
            try:
                test_choice = int(input("\nSelecciona prueba: ")) - 1
                if 0 <= test_choice < len(tests):
                    tests[test_choice][1]()
                else:
                    print("❌ Opción inválida")
            except ValueError:
                print("❌ Por favor ingresa un número válido")
                
        elif choice == "3":
            show_debug_guide()
        elif choice == "0":
            print("👋 ¡Hasta luego!")
            break
        else:
            print("❌ Opción inválida")

def show_debug_guide():
    """Mostrar guía de debug"""
    print("\n📚 GUÍA DE DEBUG PARA SISTEMA ETL")
    print("=" * 50)
    print("""
🔍 PASOS PARA HACER DEBUG:

1. VERIFICAR SERVIDOR:
   - Ejecutar: cd etl_app && python main_advanced.py
   - Verificar que responda en http://localhost:8001

2. VERIFICAR BASE DE DATOS:
   - Ejecutar migraciones: psql -f migrations/create_etl_advanced_tables.sql
   - Verificar conexión en config.py

3. USAR HERRAMIENTAS DE DEBUG:
   - Logs del servidor: revisar consola donde ejecutas main_advanced.py
   - Logs de aplicación: etl_app/logs/etl_app.log
   - Respuestas HTTP: usar esta herramienta o Postman

4. ERRORES COMUNES:
   - 500 Internal Server Error: Error en código Python o BD
   - 404 Not Found: Endpoint incorrecto o servidor no ejecutándose
   - 400 Bad Request: Datos de entrada incorrectos

5. COMANDOS ÚTILES:
   - curl -X GET http://localhost:8001/health
   - curl -X POST http://localhost:8001/api/etl/upload -F "file=@test.csv"

6. DEBUGGING CON PYTHON:
   - Agregar print() en el código
   - Usar logging.debug() para más detalles
   - Usar try/except para capturar errores específicos
""")

if __name__ == "__main__":
    main()
