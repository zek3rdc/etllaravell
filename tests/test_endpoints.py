"""
Tests para endpoints del sistema ETL avanzado
"""

import pytest
import requests
import json
from pathlib import Path
import os

# URL base para pruebas
BASE_URL = "http://localhost:8000/api/etl"

# Datos de prueba
TEST_DATA_DIR = Path(__file__).parent / "test_data"
TEST_FILE = TEST_DATA_DIR / "test_data.csv"

def setup_module():
    """Configuración inicial para las pruebas"""
    # Crear directorio de datos de prueba si no existe
    TEST_DATA_DIR.mkdir(exist_ok=True)
    
    # Crear archivo CSV de prueba
    if not TEST_FILE.exists():
        with open(TEST_FILE, 'w') as f:
            f.write("nombre,edad,email\n")
            f.write("Juan Pérez,30,juan@test.com\n")
            f.write("María García,25,maria@test.com\n")

class TestETLEndpoints:
    """Suite de pruebas para endpoints ETL"""
    
    def test_health_check(self):
        """Probar endpoint de salud"""
        response = requests.get(f"{BASE_URL}/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "queue_status" in data
    
    def test_upload_file(self):
        """Probar carga de archivo"""
        with open(TEST_FILE, 'rb') as f:
            files = {'file': f}
            response = requests.post(f"{BASE_URL}/upload", files=files)
        
        assert response.status_code == 200
        data = response.json()
        assert "session_id" in data
        assert "file_type" in data
        return data["session_id"]
    
    def test_preview_data(self):
        """Probar vista previa de datos"""
        session_id = self.test_upload_file()
        
        response = requests.post(
            f"{BASE_URL}/preview",
            data={
                "session_id": session_id,
                "sheet": "default"
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "columns" in data
        assert "preview_data" in data
        assert len(data["preview_data"]) > 0
        return session_id
    
    def test_validate_data(self):
        """Probar validación de datos"""
        session_id = self.test_preview_data()
        
        response = requests.post(
            f"{BASE_URL}/validate",
            json={
                "session_id": session_id,
                "sheet": "default"
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "validations" in data
        assert "summary" in data
        return session_id
    
    def test_custom_transformation(self):
        """Probar creación de transformación personalizada"""
        code = """
def transform(value):
    return value.upper() if isinstance(value, str) else value
"""
        
        response = requests.post(
            f"{BASE_URL}/transformations/custom",
            data={
                "name": "to_uppercase",
                "description": "Convierte texto a mayúsculas",
                "code": code
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
    
    def test_process_async(self):
        """Probar procesamiento asíncrono"""
        session_id = self.test_validate_data()
        
        response = requests.post(
            f"{BASE_URL}/process-async",
            data={
                "session_id": session_id,
                "sheet": "default",
                "column_mapping": json.dumps({"nombre": "name", "edad": "age", "email": "email"}),
                "transformations": json.dumps({
                    "name": {"type": "text", "options": {"text_transform": "upper"}}
                }),
                "target_table": "personas",
                "mode": "insert"
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "job_id" in data
        return data["job_id"]
    
    def test_job_status(self):
        """Probar estado de trabajo"""
        job_id = self.test_process_async()
        
        response = requests.get(f"{BASE_URL}/job-status/{job_id}")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
    
    def test_save_config(self):
        """Probar guardado de configuración"""
        config = {
            "name": "test_config",
            "description": "Configuración de prueba",
            "column_mapping": {"nombre": "name", "edad": "age"},
            "transformations": {
                "name": {"type": "text", "options": {"text_transform": "upper"}}
            },
            "target_table": "personas",
            "mode": "insert"
        }
        
        response = requests.post(
            f"{BASE_URL}/config/save-versioned",
            json=config
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        return config["name"]
    
    def test_config_versions(self):
        """Probar versiones de configuración"""
        config_name = self.test_save_config()
        
        response = requests.get(f"{BASE_URL}/config/{config_name}/versions")
        assert response.status_code == 200
        data = response.json()
        assert "versions" in data
        assert len(data["versions"]) > 0
    
    def test_load_history(self):
        """Probar historial de cargas"""
        response = requests.get(f"{BASE_URL}/history")
        assert response.status_code == 200
        data = response.json()
        assert "history" in data
    
    def test_load_statistics(self):
        """Probar estadísticas de cargas"""
        response = requests.get(f"{BASE_URL}/history/statistics")
        assert response.status_code == 200
        data = response.json()
        assert "general" in data
        assert "by_table" in data
    
    def test_notification_config(self):
        """Probar configuración de notificaciones"""
        config = {
            "name": "test_notification",
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
                "conditions": {
                    "only_on_errors": False,
                    "min_success_rate": 95
                }
            }
        }
        
        response = requests.post(
            f"{BASE_URL}/notifications/config",
            json=config
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
    
    def test_notification_templates(self):
        """Probar plantillas de notificación"""
        response = requests.get(f"{BASE_URL}/notifications/templates")
        assert response.status_code == 200
        data = response.json()
        assert "email" in data
        assert "slack" in data
        assert "telegram" in data
        assert "webhook" in data

if __name__ == "__main__":
    pytest.main(["-v", __file__])
