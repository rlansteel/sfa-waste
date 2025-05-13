import io
import requests
from pathlib import Path
import logging

try:
    # Intenta importar PyPDF2 y verifica si está disponible
    from PyPDF2 import PdfReader
    PYPDF2_AVAILABLE = True
except ImportError:
    print("ERROR: PyPDF2 no está instalado. Ejecuta: pip install PyPDF2")
    PYPDF2_AVAILABLE = False
    PdfReader = None # Define None para que el resto del script no falle

# Configura un logger simple
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("TestPDFReader")

# URL del PDF de SEMARNAT
PDF_URL = "https://apps1.semarnat.gob.mx:8443/dgeia/informe15/tema/pdf/Resumen15_ing.pdf"
# Opcional: Ruta local si descargaste el PDF
# PDF_LOCAL_PATH = Path("Resumen15_ing.pdf") # Descomenta y ajusta si tienes el archivo local

def test_pdf_extraction(url=None, local_path=None):
    """Intenta descargar (o leer localmente) y extraer texto de un PDF."""

    if not PYPDF2_AVAILABLE:
        logger.error("PyPDF2 no está disponible. No se puede probar la extracción.")
        return

    pdf_content = None
    source_description = ""

    # --- Obtener Contenido del PDF ---
    if local_path and local_path.exists():
        source_description = f"archivo local {local_path}"
        logger.info(f"Intentando leer PDF desde {source_description}...")
        try:
            with open(local_path, "rb") as f:
                pdf_content = io.BytesIO(f.read())
            logger.info("Archivo local leído en memoria.")
        except Exception as e:
            logger.error(f"Error leyendo archivo local {local_path}: {e}")
            return
    elif url:
        source_description = f"URL {url}"
        logger.info(f"Intentando descargar PDF desde {source_description}...")
        headers = {'User-Agent': 'Mozilla/5.0 (Test Script)'}
        verify_ssl = not ('semarnat.gob.mx' in url) # Deshabilitar verificación SSL si es necesario
        if not verify_ssl:
            logger.warning(f"Deshabilitando verificación SSL para URL: {url}")
            try:
                requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
            except AttributeError: # Manejar si urllib3 no está como se espera
                 logger.warning("No se pudo deshabilitar InsecureRequestWarning.")

        try:
            response = requests.get(url, headers=headers, timeout=30, stream=True, verify=verify_ssl)
            response.raise_for_status()
            pdf_content = io.BytesIO(response.content)
            logger.info("PDF descargado exitosamente.")
            response.close()
        except Exception as e:
            logger.error(f"Error descargando PDF desde {url}: {e}")
            if 'response' in locals(): response.close()
            return
    else:
        logger.error("Se debe proporcionar una URL o una ruta local.")
        return

    # --- Intentar Extraer Texto con PyPDF2 ---
    if pdf_content:
        logger.info(f"Intentando extraer texto del PDF ({source_description}) usando PyPDF2...")
        try:
            reader = PdfReader(pdf_content)
            logger.info(f"PDF abierto. Número de páginas: {len(reader.pages)}")

            # Intentar acceder a metadatos (puede fallar en PDFs encriptados antes de decrypt)
            try:
                metadata = reader.metadata
                if metadata and metadata.title:
                    logger.info(f"Título (metadatos): {metadata.title}")
                else:
                    logger.info("No se encontró título en metadatos.")
            except Exception as meta_err:
                 logger.warning(f"No se pudieron leer los metadatos (¿encriptado?): {meta_err}")


            # Verificar si está encriptado y intentar desencriptar si es necesario
            if reader.is_encrypted:
                logger.warning("El PDF está encriptado. Intentando desencriptar con contraseña vacía...")
                try:
                    if reader.decrypt('') != 1: # 1: success, 2: incorrect password
                        logger.error("Falló la desencriptación con contraseña vacía.")
                        # Podríamos intentar otras contraseñas comunes si fuera aplicable
                        return
                    else:
                        logger.info("Desencriptación con contraseña vacía exitosa.")
                except NotImplementedError as nie:
                     # Esto captura el error "PyCryptodome is required for AES algorithm"
                     logger.error(f"Error de dependencia durante la desencriptación: {nie}")
                     logger.error("Asegúrate de que 'pycryptodome' esté instalado correctamente en el entorno virtual.")
                     return
                except Exception as decrypt_err:
                    logger.error(f"Error inesperado durante la desencriptación: {decrypt_err}")
                    return

            # Extraer texto de las primeras páginas
            text_parts = []
            max_pages_test = 3 # Extraer solo unas pocas páginas para la prueba
            num_pages_to_read = min(len(reader.pages), max_pages_test)
            logger.info(f"Extrayendo texto de las primeras {num_pages_to_read} páginas...")
            for i in range(num_pages_to_read):
                try:
                    page = reader.pages[i]
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text.strip())
                    else:
                        text_parts.append(f"[Página {i+1} sin texto extraíble]")
                except Exception as page_err:
                    logger.error(f"Error extrayendo texto de la página {i+1}: {page_err}")
                    text_parts.append(f"[Error en página {i+1}]")

            full_text = "\n---\n".join(text_parts)
            logger.info("Extracción de texto completada.")
            print("\n--- INICIO TEXTO EXTRAÍDO (PRIMERAS PÁGINAS) ---")
            print(full_text[:1000] + ("..." if len(full_text) > 1000 else "")) # Mostrar snippet
            print("--- FIN TEXTO EXTRAÍDO ---")

        except Exception as e:
            logger.error(f"Error general al procesar el PDF con PyPDF2: {e}", exc_info=True) # Log con traceback

if __name__ == "__main__":
    # Cambia entre probar URL o archivo local descomentando la línea apropiada
    test_pdf_extraction(url=PDF_URL)
    # test_pdf_extraction(local_path=PDF_LOCAL_PATH) # Si descargaste el archivo
