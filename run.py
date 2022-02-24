from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from get_categories import list_of_categories
from datetime import datetime
from bs4 import BeautifulSoup
import logging
import zipfile
import smtplib
import random
import socket
import json
import time
import csv
import os


config_data: dict  # Глобальный словарь с параметрами из конфиг-файла


def start_parsing():
    """
    Запуск парсинга
    """
    with open('config.json') as config_file:
        global config_data
        config_data = json.load(config_file)

    # Подключаем и настраиваем логгер
    os.makedirs(config_data["logs_dir"], exist_ok=True)
    logfile = f'{config_data["logs_dir"]}/log_{datetime.now().strftime("%Y%m%d%H%M%S")}.log'
    log = logging.getLogger()
    log.setLevel(logging.INFO)
    file_handler = logging.FileHandler(logfile)
    basic_formater = logging.Formatter('%(asctime)s : [%(levelname)s] : %(message)s')
    file_handler.setFormatter(basic_formater)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(basic_formater)
    log.addHandler(file_handler)
    log.addHandler(console_handler)
    log.info('Парсер запущен')

    # Запускаем драйвер, переходим на исходную страницу
    driver = webdriver.Chrome(ChromeDriverManager().install())
    driver.set_window_size(1280, 720)
    driver.get(f'{config_data["base_url"]}/category')
    assert 'Ярче Плюс' in driver.title
    categories_list = list_of_categories()  # получаем список всех категорий

    # Начинаем парсинг для каждого из адресов
    for address in config_data['tt_id']:
        if address not in config_data['categories']:
            log.warning(f'Для "{address}" не заданы категории')
            continue

        parsed_data_head: list = [['parser_id', 'chain_id', 'tt_id', 'tt_region', 'tt_name', 'price_datetime', 'price',
                                   'price_promo', 'price_card', 'price_card_promo', 'promo_start_date',
                                   'promo_end_date', 'promo_type', 'in_stock', 'sku_status', 'sku_barcode',
                                   'sku_article', 'sku_name', 'sku_category', 'sku_brand', 'sku_country',
                                   'sku_manufacturer', 'sku_package', 'sku_packed', 'sku_weight_min', 'sku_volume_min',
                                   'sku_quantity_min', 'sku_fat_min', 'sku_alcohol_min', 'sku_link', 'api_link',
                                   'sku_parameters_json', 'sku_images', 'server_ip', 'parser_date', 'models_date',
                                   'promodata']]

        # Выбираем нужный адрес на странице
        driver.find_element(By.CLASS_NAME, 'a31qlM9dd').send_keys(Keys.ENTER)
        time.sleep(actions_delay())
        current_element = driver.find_element(By.ID, 'receivedAddress')
        current_element.send_keys(Keys.CONTROL + 'a')
        current_element.send_keys(Keys.DELETE)
        current_element.send_keys(address)
        current_element.send_keys(Keys.PAGE_DOWN)
        current_element.send_keys(Keys.ENTER)
        time.sleep(2)
        WebDriverWait(driver, 10).until(expected_conditions.element_to_be_clickable((By.CLASS_NAME, 'h3lj2Hbsk')))
        driver.find_element(By.CLASS_NAME, 'h3lj2Hbsk').send_keys(Keys.ENTER)
        time.sleep(2)

        # Проверяем наличие ids и urls для данного адреса в конфиге, если их нет - парсим все категории
        if not config_data['categories'][address]['urls'] and not config_data['categories'][address]['ids']:
            for items in categories_list:
                if items[1] != '':
                    item_url = items[3]
                    log.info(f'Парсинг {items[2]}')
                    parsed_data_head.extend(get_products_data(driver, address, item_url))

        # Если для адреса есть url в конфиге - парсим их
        if config_data['categories'][address]['urls']:
            check_list = []
            for items in categories_list:
                check_list.append(items[3])
                # Проверяем является ли url родительским, если нет - парсим только его, да - парсим дочерние
                if items[3] in config_data['categories'][address]['urls'] and items[4] != '':
                    log.info(f'Парсинг: id - {items[0]}, категория - "{items[2]}", tt_id - "{address}"')
                    parsed_data_head.extend(get_products_data(driver, address, items[3]))
                elif items[3] in config_data['categories'][address]['urls'] and items[4] == '':
                    for val in categories_list:
                        if val[4] == items[3]:
                            log.info(f'Парсинг: id - {items[0]}, категория - "{val[2]}", tt_id - "{address}"')
                            parsed_data_head.extend(get_products_data(driver, address, val[3]))

            for cat_url in config_data['categories'][address]['urls']:  # Записываем в лог недействительные urls
                if cat_url not in check_list:
                    log.warning(f'Адрес не доступен: "{cat_url}"')

        # Если для адреса есть ids в конфиге - парсим их
        if config_data['categories'][address]['ids']:
            check_list = []
            for items in categories_list:
                check_list.append(items[0])
                # Проверяем является ли id родительским, если нет - парсим только его, да - парсим дочерние
                if items[0] in config_data['categories'][address]['ids'] and items[1] != '':
                    log.info(f'Парсинг: id - {items[0]}, категория - "{items[2]}", tt_id - "{address}"')
                    parsed_data_head.extend(get_products_data(driver, address, items[3]))
                elif items[0] in config_data['categories'][address]['ids'] and items[1] == '':
                    for val in categories_list:
                        if val[1] == items[0]:
                            log.info(f'Парсинг: id - {items[0]}, категория - "{val[2]}", tt_id - "{address}"')
                            parsed_data_head.extend(get_products_data(driver, address, val[3]))

            for cat_id in config_data['categories'][address]['ids']:  # Записываем в лог недействительные ids
                if cat_id not in check_list:
                    log.warning(f'id не доступен: {cat_id}')

        # Записывем конечные данные в файл
        os.makedirs(config_data["output_directory"], exist_ok=True)
        with open(f'{config_data["output_directory"]}/{address}.csv', 'w', ) as file:
            log.info(f'Записываем данные в файл: "{address}.csv"')
            writer = csv.writer(file, delimiter=';')
            writer.writerows(parsed_data_head)

    # Архивируем данные
    zip_file = zipfile.ZipFile(f'{config_data["output_directory"]}/parsing_results.zip', 'w')
    log.info('Записываем данные в архив: parsing_results.zip')
    for folder, subfolders, files in os.walk(f'{config_data["output_directory"]}'):
        for file in files:
            if file.endswith('.csv'):
                zip_file.write(os.path.join(folder, file), file, compress_type=zipfile.ZIP_DEFLATED)
    zip_file.close()

    log.info('Отправляем почту')
    try:
        send_email(config_data['mail_data'][0], config_data['mail_data'][1], config_data['mail_data'][2],
                   config_data['mail'], f'{config_data["output_directory"]}/parsing_results.zip')
    except:
        log.error('Ошибка отправки почты')

    log.info('Завершение работы парсера')


def get_page_json(page_html: str) -> dict:
    """
    Достаём json из кода страницы
    """
    soup = BeautifulSoup(page_html, "html.parser")
    page_raw_data = soup.find('script', charset="UTF-8")
    clear_page_json = json.loads(str(page_raw_data)[49:-10])
    return clear_page_json


def actions_delay() -> int:
    """
    Получаем задержку для оперций
    """
    delay_range_s = 0 if config_data['delay_range_s'] <= 0 else random.randint(1, config_data['delay_range_s'])
    return delay_range_s


def format_string(string: str) -> str:
    """
    Убираем лишние/запрещенные символы из строк
    """
    string = string.replace('«', '')
    string = string.replace('»', '')
    string = string.replace('&nbsp', ' ')
    string = string.replace('\n', ' ')
    string = string.replace(';', ',')
    string = string.replace('\"', '')
    string = string.replace('"', '')
    string = string.replace('\xa0', ' ')
    string = string.replace('(', '')
    string = string.replace(')', '')
    string = string.replace('\xc2', ' ')
    string = string.replace('\r', '')
    string = string.replace('\t', '')
    string = string.replace('”', '')
    string = string.replace('“', '')
    return string


def get_products_data(driver, address: str, url: str) -> list:
    """
    Получаем список с собранными данными товаров
    """
    parsed_data_list = []
    server_ip = socket.gethostbyname(socket.gethostname())
    driver.get(f'{config_data["base_url"]}/{url}')
    category_page_json = get_page_json(driver.page_source)
    products_list = category_page_json['api']['productList']['list']

    # Проверяем наличие кнопки "Показать ещё", если есть - подгружаем ещё товары
    button_check = True
    while button_check is True:
        try:
            driver.find_element(By.CLASS_NAME, 'b3G7Ab9Kf').send_keys(Keys.ENTER)
            time.sleep(actions_delay())
            driver.execute_script(f'window.open("{driver.current_url}")')
            driver.switch_to.window(driver.window_handles[1])
            category_page_json = get_page_json(driver.page_source)
            products_list.extend(category_page_json['api']['productList']['list'])
            driver.close()
            driver.switch_to.window(driver.window_handles[0])
            time.sleep(actions_delay())
        except NoSuchElementException:
            button_check = False

    products = driver.find_elements(By.CLASS_NAME, 'c3s8K6a5X')

    # Проверяем товары. Если нужны только акционные - обычные пропускаем
    for i in range(len(products)):
        if config_data['promo_only'] is True:
            if products_list[i]['quant']['previousPricePerUnit'] is None:
                continue

        products[i].find_element(By.CLASS_NAME, 'g2mGXj5-x').send_keys(Keys.CONTROL + Keys.ENTER)
        driver.switch_to.window(driver.window_handles[1])
        time.sleep(actions_delay())

        # Если открыты отзывы - переключаем на описание
        try:
            buttons = driver.find_elements(By.CLASS_NAME, 'd3RbcenMm')
            buttons[1].send_keys(Keys.ENTER)
        except IndexError:
            pass

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        sku_page_params = soup.find_all('div', class_='c3Krz3-aH')

        parsed_data = [config_data['parser_id'], config_data['chain_id'], address, config_data['tt_region'],
                       f'"{config_data["chain_name"]} ({address})"',
                       datetime.now().strftime("%Y-%m-%d %H:%M:%S")]

        # Проверяем правильность цен
        if products_list[i]['quant']['previousPricePerUnit'] is not None:
            if products_list[i]['quant']['previousPricePerUnit'] > products_list[i]['quant']['pricePerUnit']:

                if type(products_list[i]['quant']['previousPricePerUnit']) == float:
                    price = ("%.1f" % products_list[i]['quant']['previousPricePerUnit'])
                    if '.0' in str(price):
                        parsed_data.append(int(str(price)[:-2]))
                    else:
                        parsed_data.append(price)
                else:
                    parsed_data.append(products_list[i]['quant']['previousPricePerUnit'])

                if type(products_list[i]['quant']['pricePerUnit']) == float:
                    price = ("%.1f" % products_list[i]['quant']['pricePerUnit'])
                    if '.0' in str(price):
                        parsed_data.append(int(str(price)[:-2]))
                    else:
                        parsed_data.append(price)
            else:
                parsed_data.append('')
                parsed_data.append('')
        else:
            if type(products_list[i]['quant']['pricePerUnit']) == float:
                price = ("%.1f" % products_list[i]['quant']['pricePerUnit'])
                if '.0' in str(price):
                    parsed_data.append(int(str(price)[:-2]))
                else:
                    parsed_data.append(price)
            else:
                parsed_data.append('')

        parsed_data.append('')  # price_card
        parsed_data.append('')  # price_card_promo
        parsed_data.append('')  # promo_start_date
        parsed_data.append('')  # promo_end_date
        parsed_data.append('')  # promo_type
        parsed_data.append('')  # in_stock

        if products_list[i]['isAvailable'] is True:
            parsed_data.append('1')  # sku_status
        else:
            parsed_data.append('0')

        parsed_data.append('')  # sku_barcode

        if 'арт.' in products_list[i]['name']:
            index = products_list[i]['name'].find('арт.')
            article = products_list[i]['name'][index+5::].split(' ')[0]
            parsed_data.append(article)
        elif 'Арт.' in products_list[i]['name']:
            index = products_list[i]['name'].find('Арт.')
            article = products_list[i]['name'][index + 5::].split(' ')[0]
            parsed_data.append(article)
        else:
            parsed_data.append('')  # sku_article

        parsed_data.append(format_string(products_list[i]['name']))  # sku_name

        try:
            parsed_data.append(f'{products_list[i]["categories"][0]["name"]} | '
                               f'{products_list[i]["categories"][1]["name"]}')  # sku_category
        except TypeError:
            parsed_data.append('')

        sku_dict = {'sku_brand': '',
                    'sku_country': '',
                    'sku_manufacturer': '',
                    'sku_package': '',
                    'sku_packed': '',
                    'sku_weight_min': '',
                    'sku_volume_min': '',
                    'sku_quantity_min': '',
                    'sku_fat_min': '',
                    'sku_alcohol_min': ''
                    }

        buffer_dict = {'Торговая марка': 'sku_brand',
                       'Страна производства': 'sku_country',
                       'Производитель': 'sku_manufacturer',
                       'Упаковка': 'sku_package',
                       'Тип товара': 'sku_packed',
                       'Вес': 'sku_weight_min',
                       'Объем': 'sku_volume_min',
                       'Кол-во': '',
                       'Жирность': '',
                       'Крепость': ''
                       }

        parameters = {}

        # Парсим основные параметры товара
        for values in sku_page_params:
            string = values.get_text(separator='|', strip=True)
            value_list = string.split('|')
            parameters.update({value_list[0]: value_list[1]})
            if value_list[0] in buffer_dict.keys():
                sku_dict[buffer_dict[value_list[0]]] = format_string(value_list[1])

        parameters = json.dumps(parameters, ensure_ascii=False)

        realisation_type = soup.find('div', class_='c34tOzx2Q')

        if 'шт' in realisation_type.text:
            if sku_dict['sku_weight_min'] == '':
                sku_dict['sku_packed'] = 2
            else:
                sku_dict['sku_packed'] = 1
        elif 'кг' in realisation_type.text:
            sku_dict['sku_packed'] = 0

        if '%' in products_list[i]['name']:
            name_list = products_list[i]['name'].split(' ')
            for j in range(len(name_list)):
                if '%' in name_list[j]:
                    sku_dict['sku_fat_min'] = name_list[j].replace('%', '')

        if 'шт' in products_list[i]['name']:
            name_list = products_list[i]['name'].split(' ')
            for j in range(len(name_list)):
                if 'шт' in name_list[j]:
                    sku_dict['sku_quantity_min'] = name_list[j].replace('шт', '')

        weight_val = sku_dict['sku_weight_min']
        if 'г' in weight_val:
            sku_dict['sku_weight_min'] = weight_val.replace('г', '')
        elif 'кг' in weight_val:
            sku_dict['sku_weight_min'] = int(float(weight_val.replace('кг', ''))*1000)

        volume_val = sku_dict['sku_volume_min']
        if 'мл' in sku_dict['sku_volume_min']:
            sku_dict['sku_volume_min'] = volume_val.replace('мл', '')
        elif 'л' in sku_dict['sku_volume_min']:
            sku_dict['sku_volume_min'] = int(float(volume_val.replace('л', '')) * 1000)

        if config_data['sku_parameters_enable'] is True:
            parsed_data.extend([sku_dict['sku_brand'], sku_dict['sku_country'], sku_dict['sku_manufacturer'],
                                sku_dict['sku_package'], sku_dict['sku_packed'], sku_dict['sku_weight_min'],
                                sku_dict['sku_volume_min'], sku_dict['sku_quantity_min'],
                                sku_dict['sku_fat_min'], sku_dict['sku_alcohol_min']])
        else:
            parsed_data.extend(['', '', '', '', '', '', '', '', '', ''])

        parsed_data.append(driver.current_url)
        parsed_data.append('')  # api_link
        parsed_data.append(parameters)  # sku_parameters_json
        picture_soup = soup.find('img', class_='c1uCMShdi')

        if config_data['sku_image_enable'] is True:
            try:
                parsed_data.append(picture_soup['src'])  # sku_images
            except TypeError:
                parsed_data.append('')
        else:
            parsed_data.append('')

        parsed_data.append(server_ip)  # server_ip
        parsed_data.append('')  # parser_date
        parsed_data.append('')  # models_date
        parsed_data.append('promodata')  # promodata
        driver.close()
        driver.switch_to.window(driver.window_handles[0])
        time.sleep(actions_delay())

        if parsed_data is not None:
            parsed_data_list.append(parsed_data)

    return parsed_data_list


def send_email(login: str, password: str, smtp: list, emails_list: list, path_to_file: str):
    """
    Метод для отправки почты
    :param login: Логин
    :param password: Пароль
    :param smtp: [host, port]
    :param emails_list:
    :param path_to_file: Путь к zip файлу с данными
    """
    message = MIMEMultipart('msg')
    message['Subject'] = 'Parsing results'
    message['From'] = login
    with open(path_to_file, 'rb') as file:
        message.attach(MIMEApplication(file.read(), Name='parsing_results.zip'))
    new_smtp = smtplib.SMTP_SSL(smtp[0], smtp[1])
    new_smtp.login(login, password)
    for email in emails_list:
        message['To'] = email
        new_smtp.sendmail(login, email, message.as_string())
    new_smtp.quit()


if __name__ == '__main__':
    start_parsing()
