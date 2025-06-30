@echo off
echo Instalando ETL Tool...

REM Crear entorno virtual
python -m venv venv
call venv\Scripts\activate

REM Instalar dependencias
pip install -r requirements.txt

REM Crear directorio para archivos temporales
mkdir temp_uploads

REM Crear base de datos y tabla de configuraciones
psql -U postgres -d jupe -f migrations/create_etl_configs_table.sql

echo.
echo Instalación completada!
echo Para iniciar la herramienta ETL, ejecute start_etl.bat
pause
