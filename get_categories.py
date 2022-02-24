from bs4 import BeautifulSoup
import requests
import json
import csv


def get_config() -> dict:
    """
    Получаем параметры из конфиг-файла
    """
    with open('config.json') as config_file:
        config_data = json.load(config_file)
    return config_data


def list_of_categories() -> list:
    """
    Составляем список возможных для парсинга категорий в виде: [[id, parent_id, name, url, parent_url]]
    """
    categories_list: list = []

    config_data = get_config()

    headers_raw = config_data['headers'].split(':')
    headers = {headers_raw[0]: headers_raw[1][1::]}

    response = requests.get('https://yarcheplus.ru/category', headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")
    page_raw_data = soup.find('script', charset="UTF-8")
    page_json = json.loads(str(page_raw_data)[49:-10])

    for category in page_json['api']['categoryList']['list']:
        parent_url = f'catalog/{category["code"]}-{category["id"]}'
        categories_list.append([category['id'], '', category['name'], parent_url, ''])

        for children in category['children']:
            child_url = f'catalog/{children["code"]}-{children["id"]}'
            categories_list.append([children['id'], category['id'], f'{category["name"]} | {children["name"]}',
                                    child_url, parent_url])

    return categories_list


def categories_to_csv():
    """
    Сохраняем список возможных для парсинга категорий в csv файл
    """
    # config_data = get_config()
    # os.makedirs(config_data["output_directory"], exist_ok=True)
    # with open(f'{config_data["output_directory"]}/categories-list.csv', 'w') as file:
    with open('categories-list.csv', 'w') as file:
        writer = csv.writer(file, delimiter=';')
        data = list_of_categories()
        data.insert(0, ['id', 'parent_id', 'name', 'url', 'parent_url'])
        writer.writerows(data)


if __name__ == '__main__':
    categories_to_csv()
