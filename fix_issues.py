import psycopg2
import requests

# Verificar y arreglar base de datos
def fix_database():
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="jupe", 
            user="postgres",
            password="12345678",
            port=5432
        )
        cur = conn.cursor()
        
        # Agregar columna code si no existe
        cur.execute("""
            ALTER TABLE etl_custom_transformations 
            ADD COLUMN IF NOT EXISTS code TEXT;
        """)
        
        conn.commit()
        print("✅ Base de datos actualizada correctamente")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error en base de datos: {e}")

# Verificar servidor
def check_server():
    try:
        response = requests.get("http://localhost:8001/health")
        if response.status_code == 200:
            print("✅ Servidor funcionando")
        else:
            print(f"❌ Servidor responde con código: {response.status_code}")
    except:
        print("❌ Servidor no responde")

if __name__ == "__main__":
    print("🔧 Arreglando problemas...")
    fix_database()
    check_server()
