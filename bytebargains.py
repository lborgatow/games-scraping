#####################################################################################################
# =================================== Importação das bibliotecas ===================================
#####################################################################################################

import requests
import time
import re
import json
from typing import List, Dict, Tuple, Any, Union
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from ratelimit import limits, sleep_and_retry
from fake_useragent import UserAgent

#####################################################################################################
# ==================================== Definição das constantes =====================================
#####################################################################################################

ua = UserAgent()
HEADERS = {'User-Agent': ua.random}
EXCLUDED_KEYWORDS = ['demo', 'trial', 'playtest', 'beta', 'dlc', 'soundtrack', 'trailer', 'movie', 'server']
STEAM_APP_LIST_URL_1 = 'http://api.steampowered.com/ISteamApps/GetAppList/v0002/'
STEAM_APP_LIST_URL_2 = 'http://api.steampowered.com/ISteamApps/GetAppList/v2/'
STEAM_APP_DETAILS_URL = 'https://store.steampowered.com/api/appdetails'
FILE_ALL_STEAM_DETAILS = './data/all_steam_details.json'
FILE_ALL_GAMES = './data/all_games.json'

#####################################################################################################
# ==================================== Definição das funções - 1 ====================================
#####################################################################################################

def check_app_name(app_name: str, excluded_keywords: List[str]) -> bool:
    """Checar se o nome do app possui alguma keyword da lista "excluded_keywords". 

    Args:
        app_name (str): String com o nome do app.
        excluded_keywords (List[str]): Lista de strings com as "keywords".

    Returns:
        bool: Se tiver a keyword no nome, retorna "False". Se não tiver, retorna "True".
    """
    return all(keyword not in app_name.lower() for keyword in excluded_keywords)


def remove_duplicates(data: List[Dict[Any, Any]]) -> List[Dict[Any, Any]]:
    """Remover elementos duplicados de uma lista de dicionários.

    Args:
        data (List[Dict[str, str]]): Lista de dicionários.

    Returns:
        List[Dict[str, str]]: Lista de dicionário sem elementos duplicados.
    """
    
    unique_keys = set()
    unique_list = []

    for d in data:
        key = tuple(sorted(d.items()))
        if key not in unique_keys:
            unique_keys.add(key)
            unique_list.append(d)

    return unique_list


def merge_and_remove_duplicates(list1: List[Dict[str, str]], 
                                list2: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Unir e remover duplicatas de duas listas de dicinários.

    Args:
        list1 (List[Dict[str, str]]): Lista de dicionários 1.
        list2 (List[Dict[str, str]]): Lista de dicionários 2.

    Returns:
        List[Dict[str, str]]: Lista de dicionários final, sem duplicatas.
    """
    
    unique_appids = set()

    list1_without_duplicates = [
        item for item in list1 if (item['appid'] not in unique_appids) and not unique_appids.add(item['appid'])
    ]

    list2_without_duplicates = [
        item for item in list2 if (item['appid'] not in unique_appids) and not unique_appids.add(item['appid'])
    ]

    final_list = list1_without_duplicates + list2_without_duplicates

    return final_list


def read_json(file_path: str) -> Any:
    """Ler um arquivo json.

    Args:
        file_path (str): String com o caminho do arquivo.

    Returns:
        Any: Conteúdo do arquivo json.
    """
    with open(file_path, 'r') as file:
        return json.load(file)
 
    
def write_json(data: Any, file_path: str) -> None:
    """Escrever em um arquivo json.

    Args:
        data (Any): Conteúdo a ser escrito no arquivo.
        file_path (str): String com o caminho do arquivo.
        
    Returns:
        None
    """
    
    with open(file_path, 'w') as file:
        json.dump(data, file)

#####################################################################################################
# ======================================== Variáveis globais ========================================
#####################################################################################################

games_steam = []
games_nuuvem = []
games_gamersgate = []
games_gog = []
all_details_steam = read_json(FILE_ALL_STEAM_DETAILS)

#####################################################################################################
# ==================================== Definição das funções - 2 ====================================
#####################################################################################################

@sleep_and_retry
@limits(calls=195, period=310)
def get_steam_response(url: str) -> Union[Dict, None]:
    """Obter o response da API da Steam.

    Args:
        url (str): String com a URL da API.

    Returns:
        Union[Dict, None]: Retorna um dicionário (json) caso o status_code da requisição
        seja 200. Caso contrário, retorna None.
    """
    with requests.Session() as session:
        response = session.get(url, timeout=20, headers=HEADERS)
        if response.status_code == 200:
            return response.json()
        else:
            return None


def get_steam_apps(url: str) -> List[Dict[str, str]]:
    """Obter os apps do response da API da Steam.

    Args:
        url (str): tring com a URL da API.

    Returns:
        List[Dict[str, str]]: Lista de dicionários com os apps.
    """
    
    app_list_data = get_steam_response(url)
    if app_list_data is None:
        return None

    return [
        {'appid': str(app['appid']), 'name': app['name']}
        for app in app_list_data['applist']['apps']
        if (app.get('name')) and (check_app_name(app['name'], EXCLUDED_KEYWORDS))
    ]


def get_steam_prices(apps: List[Dict[str, str]], appsids: List[str], slice_1: int, 
                     slice_2: int) -> List[Dict[str, Union[str, float]]]:
    """Obter os preços dos apps da Steam.

    Args:
        apps (List[Dict[str, str]]): Lista de dicionários com os apps.
        appsids (List[str]): Lista com os IDs dos apps.
        slice_1 (int): Defina o elemento inicial da fatia de IDs.
        slice_2 (int):  Defina o elemento final da fatia de IDs.

    Returns:
        List[Dict[str, Union[str, float]]]: Lista de dicionários com as informações (id,
        nome, url, preços) dos apps da Steam.
    """
    
    values_as_string = ','.join(map(str, appsids[slice_1:slice_2]))
    apps_details = None
    while apps_details is None:
        apps_details = get_steam_response(f'{STEAM_APP_DETAILS_URL}?appids={values_as_string}&cc=BR&filters=price_overview')
    apps_slice = []
    apps_dict = {app['appid']: app for app in apps}
    for key, value in apps_details.items():
        if key in apps_dict:
            app = apps_dict[key]
            appid = app['appid']
            name = app['name']  
        if value and value['success'] and value['data'] and value['data'] is not None:
            try:     
                final_price = float(value['data']['price_overview']['final']) / 100       
                discount = float(value['data']['price_overview']['discount_percent']) / 100
                initial_price = float(value['data']['price_overview']['initial']) / 100
            except ValueError:
                final_price = 0.0
                discount = 0.0
                initial_price = 0.0
        
            apps_slice.append(
                {
                    'appid': appid,
                    'name': name,
                    'href': f'https://store.steampowered.com/app/{appid}',
                    'initial_price': initial_price,
                    'final_price': final_price,
                    'discount': discount,
                }
            )
    
    return apps_slice


def process_slice_steam(apps: List[Dict[str, str]], appsids: List[str], slice_1: int, 
                     slice_2: int, lock: Lock) -> None:
    """Unir a lista de preços dos apps da fatia atual com a lista global de jogos da Steam.

    Args:
        apps (List[Dict[str, str]]): Lista de dicionários com os apps.
        appsids (List[str]): Lista com os IDs dos apps.
        slice_1 (int): Defina o elemento inicial da fatia de IDs.
        slice_2 (int):  Defina o elemento final da fatia de IDs.
        lock (Lock): Auxiliar para evitar problemas com Threads.
    
    Returns:
        None
    """
    
    global games_steam
   
    apps_prices_slice = get_steam_prices(apps, appsids, slice_1, slice_2)
    with lock:
        games_steam.extend(apps_prices_slice)
    print(f'{len(games_steam)} jogos processados')


def execute_steam_threadpool(loops: int, slice_data: int, apps: List[Dict[str, str]], 
                             appsids: List[str], last_slice: int, lock: Lock) -> None:
    """Executar as funções da Steam com ThreadPoolExecutor.

    Args:
        loops (int): Quantidade de loops (requisições).
        slice_data (int): Quantidade apps por fatia.
        apps (List[Dict[str, str]]): Lista de dicionários com os apps.
        appsids (List[str]): Lista com os IDs dos apps.
        last_slice (int): Quantidade de apps para a última fatia, caso seja necesário.
        lock (Lock): Auxiliar para evitar problemas com Threads.
        
    Returns:
        None
    """
    
    global games_steam
    
    with ThreadPoolExecutor() as executor:
        futures = []
        for i in range(loops):
            slice_1 = i * slice_data
            slice_2 = slice_1 + slice_data
            future = executor.submit(process_slice_steam, apps, appsids, slice_1, slice_2, lock)
            futures.append(future)

        if last_slice > 0:
            slice_2 = slice_1 + last_slice
            process_slice_steam(apps, appsids, slice_1, slice_2, lock)
        
        for future in as_completed(futures):
            future.result()
    

def get_new_appsids_steam(games_steam: List[Dict[str, str]], 
                          all_details_steam: List[Dict[str, str]]) -> List[str]:
    """Obter os IDs dos apps novos da Steam que ainda não foram obtidos os detalhes. 

    Args:
        games_steam (List[Dict[str, str]]): Lista de dicionários com os apps da execução atual 
        do script.
        all_details_steam (List[Dict[str, str]]): Lista de dicionários com detalhes dos apps.

    Returns:
        List[str]: Lista de strings com os IDs dos apps novos.
    """
    
    appsids_set = set(game['appid'] for game in games_steam)
    appsids = list(appsids_set)

    appsids_to_remove = {details['appid'] for details in all_details_steam if details['appid'] in appsids_set}
    appsids = [appid for appid in appsids if appid not in appsids_to_remove]
    
    return appsids


def data_unavailable_steam(appid: str) -> Dict[str, str]:
    """Obter dicionário para apps da Steam com informações indisponíveis.

    Args:
        appid (str): String com o ID do app.
    Returns:
        Dict[str, str]: Dicionário com as informações (id, tipo, gêneros, descrição e imagem)
        do app.
    """
    
    genres = []
    game_type = 'Indisponível'
    description = 'Sem descrição.'
    img = 'Sem imagem'

    return {
        'appid': appid, 
        'type': game_type, 
        'genres': genres, 
        'description': description, 
        'img': img
    }


def get_steam_app_details(app_details: Dict[str, Any], appid: str) -> Dict[str, str]:
    """Obter os detalhes de um app da Steam.

    Args:
        app_details (Dict[str, Any]): Dicionário com todos os detalhes de um app.
        appid (str): String com o ID do app.

    Returns:
        Dict[str, str]: Dicionário filtrado com os detalhes necessários de um app.
    """
    
    if app_details is not None and app_details['success'] and app_details['data']:
        game_type = app_details['data'].get('type', 'Indisponível')
        img = app_details['data'].get('header_image', 'Sem imagem')
        description = app_details['data'].get('short_description', 'Sem descrição.')
        try:
            genres = [genre['description'] for genre in app_details['data']['genres']]
        except KeyError:
            genres = []
        
        app_details_formated = {
            'appid': appid, 
            'type': game_type, 
            'genres': genres, 
            'description': description, 
            'img': img}
    else:
        app_details_formated = data_unavailable_steam(appid)

    return app_details_formated


def append_all_details_steam(appid: str, appsids: List[str]) -> None:
    """Adiciona os detalhes novos na lista global "all_details_steam".

    Args:
        appid (str): String com o ID do app.
        appsids (List[str]): Lista de strings com os IDs dos apps.

    Returns:
        None
    """
        
    global all_details_steam
    
    try:
        app_details = get_steam_response(f'{STEAM_APP_DETAILS_URL}?appids={appid}&cc=BR&l=pt')
    except requests.exceptions.JSONDecodeError:
        all_details_steam.append(data_unavailable_steam(appid))
        return None
    except requests.exceptions.ReadTimeout:
        time.sleep(310)
        try:
            app_details = get_steam_response(f'{STEAM_APP_DETAILS_URL}?appids={appid}&cc=BR&l=pt')
        except requests.exceptions.JSONDecodeError:
            all_details_steam.append(data_unavailable_steam(appid))
            return None

    while app_details is None:
        try:
            app_details = get_steam_response(f'{STEAM_APP_DETAILS_URL}?appids={appid}&cc=BR&l=pt')
            if app_details is not None:
                break
        except:
            continue

    app_details = app_details[appid]
    
    all_details_steam.append(get_steam_app_details(app_details, appid))
    
    if len(all_details_steam) == len(appsids):
        print(f'{len(appsids)} jogos novos salvos da Steam;\n')
        
    if len(all_details_steam) == len(appsids):
        print(f'{len(appsids)} jogos novos salvos da Steam;\n')


def get_all_games_steam(games_steam: List[Dict[str, Union[str, float]]], 
                        all_details_steam: List[Dict[str, str]]) -> List[Dict[str, Union[str, float]]]:
    """Juntar as informações dos jogos da lista "games_steam" com as informações de "all_details_steam".

    Args:
        games_steam (List[Dict[str, Union[str, float]]]): Lista de dicionários com informações (id, 
        nome, URL, preços) dos apps.
        all_details_steam (List[Dict[str, str]]): Lista de dicionários com informações (id, tipo, 
        descrição, gêneros, imagem) dos apps.

    Returns:
        List[Dict[str, Union[str, float]]]: Lista de dicionários com informações (id, nome, imagem,
        gêneros, descrição, URL, preços) dos jogos.
    """
    
    appsids_new = {game['appid'] for game in games_steam}

    aux_details_dict = {game['appid']: game for game in all_details_steam 
                        if game['appid'] in appsids_new}
    aux_details = [game for game in aux_details_dict.values()]

    aux_details_ordered = sorted(aux_details, key=lambda x: x['appid'])
    games_steam_ordered = sorted(games_steam, key=lambda x: x['appid'])

    return [
        {
            'id': game['appid'],
            'name': game['name'],
            'img': aux_details_ordered[i]['img'],
            'genres': aux_details_ordered[i]['genres'],
            'description': aux_details_ordered[i]['description'],
            'href': game['href'],
            'initial_price': game['initial_price'],
            'discount': game['discount'],
            'final_price': game['final_price'],
        }
        for i, game in enumerate(games_steam_ordered)
        if aux_details_ordered[i]['type'] == 'game'
    ]


def get_last_page_nuuvem(page: Any, initial_page_url: str) -> int:
    """Obter a última página de jogos da Nuuvem.

    Args:
        page (Any): Página do chromium para interações.
        page_url (str): String com a URL da página inicial.

    Returns:
        int: Número da última página de jogos.
    """

    page.goto(initial_page_url)
    last_page = int(page.query_selector('[class="pagination"]').query_selector_all('a')[-2].text_content())
    
    return last_page


def process_prices_nuuvem(game_card: Any) -> Tuple[float]:
    """Processar e tratar os preços de um jogo da Nuuvem.

    Args:
        game_card (Any): Elemento HTML que representa um jogo especiífico.

    Returns:
        Tuple[float]: Tupla de floats com o preço inicial, a porcentagem de desconto e 
        o preço final do jogo.
    """
    try:
        discount = game_card.query_selector('[class="product-discount"]').text_content()
        discount = float(''.join(re.findall(r'\d', discount))) / 100
    except AttributeError:
        discount = 0.0
    try:   
        final_price = game_card.query_selector('[class="product-button__label"]').text_content()
    except:
        # indisponível
        return None       
    try:
        final_price = float(''.join(re.findall(r'\d', final_price))) / 100
        initial_price = round(final_price / (1.0 - discount), 2)
    except ValueError:
        final_price = 0.0
        initial_price = 0.0
    
    return (initial_price, discount, final_price)
    
    
def process_game_element_nuuvem(game: Any) -> Dict[str, Union[str, float]]:
    """Processar um elemento HTML que representa um jogo da Nuuvem.

    Args:
        game (Any): Elemento HTML que representa um jogo.

    Returns:
        Dict[str, Union[str, float]]: Dicionário com as informações (id, nome,
        imagem, gêneros, URL, preços) do jogo.
    """
    
    game_id = game.get_attribute('data-track-product-sku') 
    game_card = game.query_selector('[class="product-card--wrapper"]')
    try:    
        unavailable = game_card.query_selector('[class="product-button__label"]').text_content()
    except:
        return None 
    name = game_card.get_attribute('title')
    if not check_app_name(name, EXCLUDED_KEYWORDS):
        return None
    href = game_card.get_attribute('href')
    img = game_card.query_selector('img').get_attribute('src')
    genres = [game.get_attribute('data-track-product-genre')]
    initial_price, discount, final_price = process_prices_nuuvem(game_card)
    
    return {
        'id': game_id,
        'name': name,
        'img': img,
        'genres': genres,
        'href': href,
        'initial_price': initial_price,
        'discount': discount,
        'final_price': final_price,
    }
 
    
def append_games_data_nuuvem(page: Any, last_page: int) -> None:
    """Obter as informações de cada jogo de todas as páginas de jogos da Nuuvem.

    Args:
        page (Any): Página do chromium para interações.
        last_page (int): Número da última página de jogos.
        
    Returns:
        None
    """
    
    global games_nuuvem
    
    for page_number in range(1, last_page+1):
        page_url = f'https://www.nuuvem.com/br-pt/catalog/platforms/pc/types/games/sort/title/sort-mode/asc/page/{page_number}'
        page.goto(page_url)
        games = page.query_selector_all('[data-component="product-card"]')
        for game in games:
            game_data = process_game_element_nuuvem(game)
            if game_data:
                games_nuuvem.append(game_data)
        print(f'Página {page_number} processada')


def scrape_page_gamersgate(page_url: str) -> List[Dict[str, Union[str, float]]]:
    """Realizar a raspagem de informações dos jogos de uma página da Gamersgate.

    Args:
        page_url (str): String com a URL da página.

    Returns:
        List[Dict[str, Union[str, float]]: Lista de dicionários com as informações (id, nome, 
        imagem, URL e preços) de um jogo.
    """
    
    with requests.Session() as session:
        page = session.get(page_url, timeout=20, headers=HEADERS)
        pg = BeautifulSoup(page.content, 'html.parser')
        games = pg.find_all('div', attrs={'class': 'column catalog-item product--item'})
        page_games = []
        for game in games:
            game_id = game['data-id']
            game_title = game.find('div', attrs={'class': 'catalog-item--title'}).find('a')
            name = game_title['title']
            if not check_app_name(name, EXCLUDED_KEYWORDS):
                continue
            href = 'https://gamersgate.com' + game_title['href']
            img = game.find('div', attrs={'class': 'catalog-item--image'}).find('img')['src']
            try:
                discount = game.find('li', attrs={'class': 'catalog-item--product-label-v2 product--label-discount'}).text
                discount = float(''.join(re.findall(r'\d', discount))) / 100
            except AttributeError:
                discount = 0.0
            final_price = game.find('div', attrs={'class': 'catalog-item--price'}).find('span').text
            try:
                initial_price = game.find('div', attrs={'class': 'catalog-item--full-price'}).text
            except AttributeError:
                initial_price = final_price
            try:
                final_price = float(''.join(re.findall(r'\d', final_price))) / 100
                initial_price = float(''.join(re.findall(r'\d', initial_price))) / 100
            except:
                final_price = 0.0
                initial_price = 0.0
            page_games.append(
                {
                    'id': game_id,
                    'name': name,
                    'img': img,
                    'href': href,
                    'initial_price': initial_price,
                    'discount': discount,
                    'final_price': final_price,
                }
            )
    return page_games


def process_page_gamersgate(page_number: int, page_url: str, lock: Lock) -> None:
    """Unir a lista de preços dos jogos da página atual com a lista global de jogos da Gamersgate.

    Args:
        page_number (int): Número da página atual.
        page_url (str): String com a URL da página atual.
        lock (Lock): Auxiliar para evitar problemas com Threads.
    
    Returns:
        None
    """
    
    global games_gamersgate
    
    page_games = scrape_page_gamersgate(page_url)
    with lock:
        games_gamersgate.extend(page_games)
    print(f'Página {page_number} processada')


def execute_gamersgate_threadpool(last_page: int, lock: Lock) -> None:
    """Executar as funções da Gamersgate com ThreadPoolExecutor.

    Args:
        last_page (int): Número da última página de jogos.
        lock (Lock): Auxiliar para evitar problemas com Threads.
    
    Returns:
        None
    """
    
    global games_gamersgate
    
    with ThreadPoolExecutor() as executor:
        page_urls = [f'https://www.gamersgate.com/games/?platform=pc&platform=mac&platform=linux&dlc=on&page={page_number}&sort=alphabetically&per_page=90' for page_number in range(1, last_page+1)]
        futures = [executor.submit(process_page_gamersgate, page_number, page_url, lock) for page_number, page_url in enumerate(page_urls, start=1)]

        for future in as_completed(futures):
            future.result()


def get_last_page_gog(page: Any, initial_page_url: str) -> int:
    """Obter a última página de jogos da Gog.

    Args:
        page (Any): Página do chromium para interações.
        page_url (str): String com a URL da página inicial.

    Returns:
        int: Número da última página de jogos.
    """
    
    page.goto(initial_page_url)
    last_page = int(page.query_selector_all('[selenium-id="paginationPage"]')[-1].text_content())
    
    return last_page
 
    
def process_prices_gog(final_price: str) -> Tuple[float]:
    """Processar e tratar os preços de um jogo da Gog.

    Args:
        final_price (str): String com o preço final de um jogo.

    Returns:
        Tuple[float]: Tupla de floats com o preço inicial, a porcentagem de desconto e 
        o preço final do jogo.
    """
    initial_price = final_price
    discount = 0.0            
    if (final_price != 0.0) and ('%' in final_price):
        aux = re.findall(r'(-?\d*\.?\d+%|R\$\d*\.\d*)', final_price)
        discount = float(''.join(re.findall(r'\d', aux[0]))) / 100
        initial_price = aux[1]
        final_price = aux[2]                          
    if (final_price != 0.0) and (initial_price != 0.0):
        initial_price = float(''.join(re.findall(r'\d', initial_price))) / 100
        final_price = float(''.join(re.findall(r'\d', final_price))) / 100
    
    return (initial_price, discount, final_price)
    
    
def process_game_element_gog(game: Any) -> Dict[str, Union[str, float]]:
    """Processar um elemento HTML que representa um jogo da Gog.

    Args:
        game (Any): Elemento HTML que representa um jogo.

    Returns:
        Dict[str, Union[str, float]]: Dicionário com as informações (id, nome,
        imagem, URL, preços) do jogo.
    """
    
    game_id = game.get_attribute('data-product-id')           
    name = game.query_selector('[selenium-id="productTitle"]').text_content().strip()
    if not check_app_name(name, EXCLUDED_KEYWORDS):
        return None            
    try:
        img = game.query_selector_all('[type="image/jpeg"]')[0].get_attribute('srcset').split(',')[0]
    except AttributeError:
        img = game.query_selector_all('[type="image/jpeg"]')[0].get_attribute('lazyload').split(',')[0]           
    href = game.get_attribute('href')           
    final_price = game.query_selector('[selenium-id="productPrice"]').text_content()           
    if final_price == 'FREE':
        final_price = 0.0
    initial_price, discount, final_price = process_prices_gog(final_price)
    
    return {
        'id': game_id,
        'name': name,
        'img': img,
        'href': href,
        'initial_price': initial_price,
        'discount': discount,
        'final_price': final_price,
    }
 
    
def append_games_data_gog(page: Any, last_page: int) -> None:
    """Obter as informações de cada jogo de todas as páginas de jogos da Gog.

    Args:
        page (Any): Página do chromium para interações.
        last_page (int): Número da última página de jogos.
        
    Returns:
        List[Dict[str, Union[str, float]]]: Lista de dicionários com as informações (id, nome,
        imagem, URL, preços) de cada jogo.
    """
    
    global games_gog
    
    for page_number in range(1, last_page+1):
        page_url = f'https://www.gog.com/en/games?order=asc:title&hideDLCs=true&excludeReleaseStatuses=upcoming&page={page_number}'
        page.goto(page_url)
        games = page.query_selector_all('[class="product-tile product-tile--grid"]')
        for game in games:
            game_data = process_game_element_gog(game)
            if game_data:
                games_gog.append(game_data)
        print(f'Página {page_number} processada')


def add_missing_shops(all_games: Dict[str, Dict[str, Any]], 
                      shops: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Adicionar lojas ausentes para cada jogo.

    Args:
        all_games (Dict[str, Dict[str, Any]]): Dicionário com os jogos e suas informações 
        (gêneros, descrição, imagem, lojas).
        shops (Dict[str, Dict[str, Any]]): Dicionário com as lojas (chaves)
        e seus jogos (valores)

    Returns:
        Dict[str, Dict[str, Any]]: Dicionário com os jogos e suas informações (gêneros, 
        descrição, imagem, lojas) atualizado com as lojas.
    """
    
    for game_details in all_games.values():
        existent_shops = {shop['shop'] for shop in game_details['shops']}
        missing_shops = set(shops.keys()) - existent_shops

        for missing_shop in missing_shops:
            game_details['shops'].append(
                {
                    "shop": missing_shop,
                    "gameid": 'Indisponível',
                    "href": 'Indisponível',
                    "prices": {
                        "initial_price": 'Indisponível',
                        "discount": 'Indisponível',
                        "final_price": 'Indisponível'
                    }
                }
            )  
    
    return all_games 


def get_new_all_games(shops: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Obter o novo dicionário de jogos.

    Args:
        shops (Dict[str, Dict[str, Any]]): Dicionário com as lojas (chaves)
        e seus jogos (valores).

    Returns:
        Dict[str, Dict[str, Any]]: Dicionário com os jogos e suas informações (gêneros, 
        descrição, imagem, lojas).
    """
    
    all_games = {}
    for shop_name, games in shops.items():
        for game in games:
            game_name = game['name']
            if game_name in all_games:
                all_games[game_name]['shops'].append(
                    {
                        "shop": shop_name,
                        "gameid": game['id'],
                        "href": game['href'],
                        "prices": {
                            "initial_price": game['initial_price'],
                            "discount": game['discount'],
                            "final_price": game['final_price']
                        }
                    }
                )
            else:
                all_games[game_name] = {
                    "genres": game.get('genres', ['Indisponível']),
                    "description": game.get('description', 'Sem descrição.'),
                    "img": game['img'],
                    "shops": [
                        {
                            "shop": shop_name,
                            "gameid": game['id'],
                            "href": game['href'],
                            "prices": {
                                "initial_price": game['initial_price'],
                                "discount": game['discount'],
                                "final_price": game['final_price']
                            }
                        }
                    ]
                }
    
    all_games = add_missing_shops(all_games, shops)
                
    return all_games


def get_all_definitive_games(all_games: Dict[str, Dict[str, Any]], 
                             all_games_old: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Obter dicionário definitivo com os dados dos jogos atualizados.

    Args:
        all_games (Dict[str, Dict[str, Any]]): Dicionário com os jogos e suas informações (gêneros, 
        descrição, imagem, lojas) obtidos no script.
        all_games_old (Dict[str, Dict[str, Any]]): Dicionário com os jogos e suas informações 
        (gêneros, descrição, imagem, lojas) presente atualmente no banco de dados.

    Returns:
        Dict[str, Dict[str, Any]]: Dicionário com os jogos e suas informações (gêneros, 
        descrição, imagem, lojas) que será gravado no banco.
    """
    
    all_games_definitive = all_games_old
    
    for game_name, game_info_new in all_games.items():
        if game_name in all_games_definitive:
            for shop_new in game_info_new["shops"]:
                shop_name_new = shop_new["shop"]
                existing_shops = all_games_definitive[game_name]["shops"]

                shop_index = next((index for index, shop in enumerate(existing_shops) if shop["shop"] == shop_name_new), None)

                if shop_index is not None:
                    existing_shops[shop_index] = shop_new
        else:
            all_games_definitive[game_name] = game_info_new

    for game_name, game_info_old in all_games_definitive.items():
        if game_name not in all_games:
            for shop_old in game_info_old["shops"]:
                shop_name_old = shop_old["shop"]
                existing_shops = game_info_old["shops"]
                
                shop_index = next((index for index, shop in enumerate(existing_shops) if shop["shop"] == shop_name_old), None)

                if shop_index is not None:
                    game_info_old["shops"][shop_index] = {
                        "shop": shop_name_old,
                        "gameid": 'Indisponível',
                        "href": 'Indisponível',
                        "prices": {
                            "initial_price": 'Indisponível',
                            "discount": 'Indisponível',
                            "final_price": 'Indisponível'
                        }
                    }
    
    return all_games_definitive


#####################################################################################################
# ============================================== MAIN ===============================================
#####################################################################################################

def main():

    global games_steam
    global games_nuuvem
    global games_gamersgate
    global games_gog
    global all_details_steam
    
    print('#' * 91)   
    print('='*40 + ' INICIADO ' + '='*41)
    print('#' * 91)

    start_script = time.time()

    #####################################################################################################
    # ============================================== STEAM ==============================================
    #####################################################################################################

    print('\n' + '='*42 + ' STEAM ' + '='*42)

    start = time.time()

    apps_1 = get_steam_apps(STEAM_APP_LIST_URL_1)
    apps_2 = get_steam_apps(STEAM_APP_LIST_URL_2)
    
    apps = merge_and_remove_duplicates(apps_1, apps_2)
    
    print(f"\n{len(apps)} recursos encontrados;\n")
    appsids = list(map(lambda d: d['appid'], apps))
    slice_data = len(appsids) // 190 # API Steam permite 200 requisições a cada 5 minutos
    loops = len(appsids) // slice_data
    last_slice = len(appsids) % slice_data
    lock = Lock()
    execute_steam_threadpool(loops, slice_data, apps, appsids, last_slice, lock)
    
    end = time.time()
    total_time = end - start
    print("\nDados da STEAM salvos com sucesso;")
    print(f"Tempo de execução: {total_time:.2f} segundos\n")

    ####################################################################################################
    #============================================= NUUVEM ==============================================
    ####################################################################################################
            
    print('='*41 + ' NUUVEM ' + '='*42)

    start = time.time()

    with sync_playwright() as p:
        browser = p.firefox.launch(headless=True)
        page = browser.new_page()
        last_page = get_last_page_nuuvem(page, 'https://www.nuuvem.com/br-pt/catalog/platforms/pc/types/games/sort/title/sort-mode/asc')
        print(f'\n{last_page} páginas encontradas;\n')
        append_games_data_nuuvem(page, last_page)
        browser.close() 
            
    end = time.time()
    total_time = end - start

    print('\nDados da NUUVEM salvos com sucesso;')
    print(f'Tempo de execução: {total_time:.2f} segundo(s)\n')

    ####################################################################################################
    #=========================================== GAMERSGATE ============================================
    ####################################################################################################
                
    print('='*39 + ' GAMERSGATE ' + '='*40)

    start = time.time()

    url = 'https://www.gamersgate.com/games/?platform=pc&platform=mac&platform=linux&dlc=on&sort=alphabetically&per_page=90'
    with requests.Session() as session:
        gamersgate = session.get(url, timeout=20, headers=HEADERS)
        initial_page = BeautifulSoup(gamersgate.content, 'html.parser')
        last_page = int(initial_page.find('div', attrs={'class': 'catalog-paginator'}).find_all('li')[-1].text)
    print(f'\n{last_page} páginas encontradas;\n')
    lock = Lock()
    execute_gamersgate_threadpool(last_page, lock)
    
    end = time.time()
    total_time = end - start

    print('\nDados da GAMERSGATE salvos com sucesso;')
    print(f'Tempo de execução: {total_time:.2f} segundo(s)\n')

    #####################################################################################################
    # =============================================== GOG ===============================================
    #####################################################################################################

    print('='*43 + ' GOG ' + '='*43)

    start = time.time()

    with sync_playwright() as p:  
        browser = p.firefox.launch(headless=True)  
        page = browser.new_page() 
        last_page = get_last_page_gog(page, 'https://www.gog.com/en/games?order=asc:title&hideDLCs=true&excludeReleaseStatuses=upcoming')
        print(f'\n{last_page} páginas encontradas;\n')    
        append_games_data_gog(page, last_page)                  
        browser.close()
    
    end = time.time()
    total_time = end - start

    print('\nDados da GOG salvos com sucesso;')
    print(f'Tempo de execução: {total_time:.2f} segundo(s)\n')

    #####################################################################################################
    # ========================================== Processamento ==========================================
    #####################################################################################################
                    
    print('='*38 + ' Processamento ' + '='*38)

    # ============================================== Steam ==============================================

    appsids = get_new_appsids_steam(games_steam, all_details_steam)
    print(f'\n{len(appsids)} recursos novos encontrados na Steam;\n')

    for appid in appsids: 
        append_all_details_steam(appid, appsids)
    
    write_json(all_details_steam, FILE_ALL_STEAM_DETAILS)
    
    games_steam = remove_duplicates(games_steam)

    all_games_steam = get_all_games_steam(games_steam, all_details_steam)

    # ============================================== Jogos ==============================================        

    print('Processando jogos de todas as lojas...\n')

    shops = {'steam': all_games_steam, 
            'nuuvem': games_nuuvem, 
            'gamersgate': games_gamersgate, 
            'gog': games_gog}

    all_games = get_new_all_games(shops)

    # Remove os jogos diferentes que possuem o mesmo nome (causa problema nas lojas)
    all_games = {game: details for game, details in all_games.items() if len(details['shops']) <= len(shops)}

    all_games_old = read_json(FILE_ALL_GAMES)

    all_games_definitive = get_all_definitive_games(all_games, all_games_old)
    print(f'{len(all_games_definitive)} jogos processados;\n')

    write_json(all_games_definitive, FILE_ALL_GAMES)

    print('#' * 91)        
    print('='*39 + ' FINALIZADO ' + '='*40)
    print('#' * 91)

    end_script = time.time()
    total_time_script = end_script - start_script

    print('\nTodos os dados processado e salvos com sucesso;')
    print(f'Tempo total de execução: {total_time_script:.2f} segundo(s)\n')

#####################################################################################################
# ============================================ Execução =============================================
#####################################################################################################

if __name__ == "__main__":
    main()