# 🚀 Sistema ETL Avanzado - Instrucciones de Instalación y Uso

## 📋 Requisitos Previos

1. **Python 3.8+** instalado
2. **PostgreSQL** ejecutándose
3. **Base de datos "jupe"** creada
4. **Acceso a internet** para descargar dependencias

## 🔧 Instalación Paso a Paso

### 1. Navegar al directorio ETL
```bash
cd etl_app
```

### 2. Instalar dependencias básicas
```bash
pip install -r requirements_simple.txt
```

### 3. Crear las tablas de base de datos
```bash
psql -U postgres -d jupe -f migrations/create_etl_advanced_tables.sql
```

### 4. Verificar configuración de base de datos
Editar `config.py` si es necesario:
```python
DATABASE_CONFIG = {
    "host": "localhost",
    "database": "jupe", 
    "user": "postgres",
    "password": "12345678",  # Tu contraseña
    "port": 5432
}
```

## 🚀 Iniciar el Sistema

### Opción 1: Script de inicio simple
```bash
python start_server.py
```

### Opción 2: Directamente con uvicorn
```bash
python -m uvicorn main_advanced:app --host 0.0.0.0 --port 8001 --reload
```

## 🧪 Probar el Sistema

### 1. Verificar que el servidor esté funcionando
Abrir en navegador: http://localhost:8001/health

### 2. Ver documentación de la API
Abrir en navegador: http://localhost:8001/docs

### 3. Usar herramienta de debug interactiva
```bash
python debug_guide.py
```

### 4. Ejecutar tests automatizados
```bash
python -m pytest tests/test_endpoints.py -v
```

## 📊 Endpoints Principales

| Endpoint | Método | Descripción |
|----------|--------|-------------|
| `/health` | GET | Estado del servidor |
| `/api/etl/upload` | POST | Cargar archivo |
| `/api/etl/validate` | POST | Validar datos |
| `/api/etl/process-async` | POST | Procesar datos |
| `/api/etl/history` | GET | Historial de cargas |
| `/docs` | GET | Documentación interactiva |

## 🔍 Debug y Solución de Problemas

### Error: "ModuleNotFoundError"
```bash
# Verificar que estés en el directorio correcto
cd etl_app

# Reinstalar dependencias
pip install -r requirements_simple.txt
```

### Error: "Connection refused" 
```bash
# Verificar que PostgreSQL esté ejecutándose
# Verificar configuración en config.py
```

### Error: "Table doesn't exist"
```bash
# Ejecutar migraciones
psql -U postgres -d jupe -f migrations/create_etl_advanced_tables.sql
```

## 📚 Ejemplos de Uso

### 1. Cargar y validar archivo CSV
```python
import requests

# Cargar archivo
files = {'file': open('data.csv', 'rb')}
response = requests.post('http://localhost:8001/api/etl/upload', files=files)
session_id = response.json()['session_id']

# Validar datos
validation = requests.post('http://localhost:8001/api/etl/validate', 
                         json={'session_id': session_id, 'sheet': 'default'})
print(validation.json())
```

### 2. Crear transformación personalizada
```python
code = """
def transform(value):
    return value.upper() if isinstance(value, str) else value
"""

response = requests.post('http://localhost:8001/api/etl/transformations/custom',
                        data={
                            'name': 'to_uppercase',
                            'description': 'Convierte a mayúsculas',
                            'code': code
                        })
```

### 3. Procesar datos asíncronamente
```python
response = requests.post('http://localhost:8001/api/etl/process-async',
                        data={
                            'session_id': session_id,
                            'sheet': 'default',
                            'column_mapping': '{"col1": "nombre"}',
                            'target_table': 'personas',
                            'mode': 'insert'
                        })

job_id = response.json()['job_id']

# Monitorear progreso
status = requests.get(f'http://localhost:8001/api/etl/job-status/{job_id}')
```

## 🎯 Funcionalidades Principales

### ✅ Validaciones Avanzadas
- Detección automática de tipos de datos
- Análisis de calidad (nulos, duplicados, outliers)
- Recomendaciones de limpieza

### ✅ Transformaciones Personalizadas
- Editor de código Python integrado
- Sandbox seguro de ejecución
- Biblioteca de funciones predefinidas

### ✅ Procesamiento Asíncrono
- Procesamiento por chunks para archivos grandes
- Cola de trabajos con prioridades
- Monitoreo en tiempo real

### ✅ Configuraciones Versionadas
- Múltiples versiones por configuración
- Comparación entre versiones
- Activación/desactivación de versiones

### ✅ Historial y Rollback
- Registro completo de operaciones
- Capacidad de revertir cambios
- Estadísticas de uso

### ✅ Sistema de Notificaciones
- Email, Slack, Telegram, Webhooks
- Plantillas personalizables
- Reglas configurables

## 🆘 Soporte

Si encuentras problemas:

1. **Revisar logs:** Consola donde ejecutas el servidor
2. **Usar debug_guide.py:** Herramienta interactiva de debug
3. **Verificar configuración:** config.py y base de datos
4. **Consultar documentación:** http://localhost:8001/docs

## 📁 Estructura de Archivos

```
etl_app/
├── main_advanced.py          # API principal
├── start_server.py          # Script de inicio
├── debug_guide.py           # Herramienta de debug
├── config.py                # Configuración
├── requirements_simple.txt  # Dependencias
├── models/                  # Modelos de datos
├── modules/                 # Módulos funcionales
├── migrations/              # Scripts de BD
└── tests/                   # Pruebas
```

¡El sistema está listo para usar! 🎉
