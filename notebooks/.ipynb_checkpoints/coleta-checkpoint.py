#bibliotecas importadas
import requests 

def obter_dados():
    """    
    Realiza uma requisição HTTP GET à Fake Store API e retorna os dados de produtos em formato JSON.
    """
    url = "https://fakestoreapi.com/products"
    return requests.get(url).json()