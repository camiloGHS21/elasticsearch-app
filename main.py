from elasticsearch import Elasticsearch

# Contraseña para el usuario 'elastic' generado por Elasticsearch
ELASTIC_PASSWORD = "123456"
SSL_CERTIFICATE = "http_ca.crt"

# Inicializa el cliente de Elasticsearch
client = Elasticsearch(
    hosts="https://localhost:9200",
    ca_certs=SSL_CERTIFICATE,
    basic_auth=("elastic", ELASTIC_PASSWORD)
)

# Recupera información sobre el clúster de Elasticsearch
info_cliente = client.info()
print(info_cliente)

# Realiza varias consultas
# Consulta 1: Buscar documentos en el índice 'usuarios'
resultados_busqueda = client.search(index="usuarios", body={"query": {"match_all": {}}})
print("\n  Resultados de búsqueda para el índice usuarios: ")
print(resultados_busqueda)

# Consulta 2: Recupera un documento específico por su ID del índice 'usuarios'
id_documento = "6YZLTo8BLJPJgaUxFBO7"
documento_obtenido = client.get(index="usuarios", id=id_documento)
print("\n  Documento recuperado del índice usuarios:")
print(documento_obtenido)

# Consulta 3: Realiza una consulta de agregación para obtener estadísticas del índice 'usuarios'
consulta_agregacion = {
    "aggs": {
        "edad_promedio": {"avg": {"field": "age"}},
        "edad_maxima": {"max": {"field": "age"}}
    }
}
resultados_agregacion = client.search(index="usuarios", body=consulta_agregacion)
# Imprime solo los resultados de la agregación
datos_agregacion = resultados_agregacion["aggregations"]
print("\n Resultados de agregación para el índice usuarios:")
print(datos_agregacion)
