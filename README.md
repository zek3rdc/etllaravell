# Sistema ETL Avanzado

Sistema ETL modular con soporte para validaciones, transformaciones personalizadas, procesamiento asíncrono, notificaciones y más.

## Características Principales

### 1. Modularidad
- Arquitectura modular con componentes independientes
- Fácil extensión y mantenimiento
- Separación clara de responsabilidades

### 2. Configuraciones Versionadas
- Soporte para múltiples versiones de configuración
- Control de versiones semántico
- Comparación entre versiones
- Activación/desactivación de versiones

### 3. Transformaciones Personalizadas
- Editor de código Python integrado
- Sandbox seguro para ejecución
- Biblioteca de funciones predefinidas
- Soporte para parámetros personalizados

### 4. Validaciones Previas
- Análisis automático de calidad de datos
- Detección de tipos, nulos y duplicados
- Reporte detallado de problemas
- Recomendaciones automáticas

### 5. Soporte para Grandes Volúmenes
- Procesamiento por chunks
- Sistema de cola asíncrona
- Monitoreo de progreso en tiempo real
- Gestión de recursos optimizada

### 6. Historial y Rollback
- Registro detallado de operaciones
- Capacidad de revertir cambios
- Estadísticas de uso
- Trazabilidad completa

### 7. Sistema de Notificaciones
- Múltiples canales (Email, Slack, Telegram)
- Plantillas personalizables
- Reglas de notificación configurables
- Registro de envíos

### 8. Soporte Multi-idioma
- Interfaz en múltiples idiomas
- Mensajes localizados
- Adaptación cultural

## Estructura del Proyecto

```
etl_app/
├── migrations/                  # Migraciones de base de datos
│   ├── create_etl_configs_table.sql
│   └── create_etl_advanced_tables.sql
├── models/                     # Modelos de datos
│   ├── __init__.py
│   ├── base.py                # Modelo base
│   ├── config.py              # Configuraciones
│   ├── history.py             # Historial
│   ├── notification.py        # Notificaciones
│   └── validation.py          # Validaciones
├── modules/                    # Módulos funcionales
│   ├── __init__.py
│   ├── validator.py           # Validador de datos
│   ├── transformer.py         # Transformador de datos
│   ├── processor.py           # Procesador principal
│   ├── notifier.py           # Gestor de notificaciones
│   └── job_manager.py        # Gestor de trabajos
├── config.py                  # Configuración general
├── main.py                    # API FastAPI básica
└── main_advanced.py          # API FastAPI avanzada
```

## Instalación

1. Crear entorno virtual:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

2. Instalar dependencias:
```bash
pip install -r requirements.txt
```

3. Configurar base de datos:
```bash
psql -U postgres -f migrations/create_etl_configs_table.sql
psql -U postgres -f migrations/create_etl_advanced_tables.sql
```

4. Configurar variables de entorno:
```bash
cp .env.example .env
# Editar .env con tus configuraciones
```

## Uso

### Iniciar el servidor:
```bash
uvicorn main_advanced:app --reload
```

### Endpoints Principales

#### Carga de Archivos
- `POST /api/etl/upload`: Cargar archivo
- `POST /api/etl/preview`: Vista previa de datos

#### Validaciones
- `POST /api/etl/validate`: Validar datos
- `GET /api/etl/validation-history/{session_id}`: Historial de validaciones

#### Transformaciones
- `GET /api/etl/transformations/available`: Listar transformaciones
- `POST /api/etl/transformations/custom`: Crear transformación personalizada

#### Procesamiento
- `POST /api/etl/process-async`: Procesar datos (asíncrono)
- `GET /api/etl/job-status/{job_id}`: Estado del trabajo
- `POST /api/etl/job-cancel/{job_id}`: Cancelar trabajo

#### Configuraciones
- `POST /api/etl/config/save-versioned`: Guardar configuración
- `GET /api/etl/config/{name}/versions`: Listar versiones
- `POST /api/etl/config/{name}/activate-version`: Activar versión

#### Historial
- `GET /api/etl/history`: Historial de cargas
- `GET /api/etl/history/statistics`: Estadísticas
- `POST /api/etl/history/{history_id}/rollback`: Revertir carga

#### Notificaciones
- `POST /api/etl/notifications/config`: Configurar notificaciones
- `GET /api/etl/notifications/templates`: Plantillas disponibles

## Ejemplos de Uso

### 1. Carga y Validación Básica
```python
import requests

# Cargar archivo
files = {'file': open('data.csv', 'rb')}
response = requests.post('http://localhost:8000/api/etl/upload', files=files)
session_id = response.json()['session_id']

# Validar datos
validation = requests.post('http://localhost:8000/api/etl/validate', 
                         json={'session_id': session_id, 'sheet': 'default'})
print(validation.json())
```

### 2. Transformación Personalizada
```python
# Crear transformación personalizada
code = """
def transform(value):
    return value.upper() if isinstance(value, str) else value
"""

response = requests.post('http://localhost:8000/api/etl/transformations/custom',
                        data={
                            'name': 'to_uppercase',
                            'description': 'Convierte texto a mayúsculas',
                            'code': code
                        })
```

### 3. Procesamiento Asíncrono
```python
# Iniciar procesamiento
response = requests.post('http://localhost:8000/api/etl/process-async',
                        data={
                            'session_id': session_id,
                            'sheet': 'default',
                            'column_mapping': '{"col1": "nombre", "col2": "edad"}',
                            'transformations': '{"nombre": {"type": "text", "options": {"text_transform": "upper"}}}',
                            'target_table': 'personas',
                            'mode': 'insert'
                        })

job_id = response.json()['job_id']

# Monitorear progreso
status = requests.get(f'http://localhost:8000/api/etl/job-status/{job_id}')
print(status.json())
```

## Contribución

1. Fork el repositorio
2. Crear rama para feature (`git checkout -b feature/AmazingFeature`)
3. Commit cambios (`git commit -m 'Add some AmazingFeature'`)
4. Push a la rama (`git push origin feature/AmazingFeature`)
5. Abrir Pull Request

## Licencia

Este proyecto está licenciado bajo la Licencia MIT - ver el archivo [LICENSE](LICENSE) para más detalles.
