import ssl
import certifi

# Обновите сертификаты
def update_ssl_certificates():
    cert_path = certifi.where()
    print(f"Certificate path: {cert_path}")
    
    # Или укажите путь вручную
    ssl_context = ssl.create_default_context(cafile="C:/Users/Azyabin/Downloads/cacert-2025-11-04.pem")
    return ssl_context