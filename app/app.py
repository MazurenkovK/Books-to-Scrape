import requests
from bs4 import BeautifulSoup
import pandas as pd
import schedule
import time


def fetch_books_data():
    base_url = 'http://books.toscrape.com/catalogue/.html'
    book_list = []
    page_number = 1

    while True:
        url = f'http://books.toscrape.com/catalogue/category/books_1/page-{page_number}.html'
        response = requests.get(url)
        response.encoding = 'utf-8'  # Устанавливаем кодировку на UTF-8

        # Проверяем ответ
        if response.status_code != 200:
            print("Ошибка при получении страницы:", response.status_code)
            break

        soup = BeautifulSoup(response.content, 'html.parser')
        books = soup.find_all('article', class_='product_pod')

        if not books:  
            print("Книги не найдены на странице.")
            return

        for book in books:
            title = book.h3.a['title']            
            book_url = base_url + book.h3.a['href']
            price = book.find('p', class_='price_color').get_text()
            price = float(price[1:]) # Возвращаем число
            star_rating = book.p['class'][1]  # Класс с рейтингом
            star_map = {'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5}
            rating = star_map.get(star_rating, 0)
            availability = book.find('p', class_='instock availability').text.strip()

            # Переходим на страницу книги для получения полной информации
            book_response = requests.get(book_url)
            book_response.encoding = 'utf-8'  # Устанавливаем кодировку на UTF-8
            book_soup = BeautifulSoup(book_response.content, 'html.parser')

            # Получаем описание и проверяем его наличие
            description = book_soup.find('meta', {'name': 'description'})
            description = description['content'].strip() if description else 'No description available'
            
            # Получаем дополнительную информацию
            product_info = book_soup.find('table', class_='table table-striped')
            additional_info = {}
            if product_info:
                rows = product_info.find_all('tr')
                for row in rows:
                    key = row.find('th').text.strip()
                    value = row.find('td').text.strip()
                    additional_info[key] = value
            else:
                print(f"Дополнительная информация не найдена для книги: {title}")


            # Общий словарь для книги
            book_data = {
                'Title': title,
                'Price_incl_tax': price,
                'Rating': rating,
                'Availability': availability,
                'Description': description,
            }
            book_data.update(additional_info)
            book_list.append(book_data)

        page_number += 1

    # Сохранение данных в DataFrame
    df = pd.DataFrame(book_list)

    # Предобработка данных
    df.drop_duplicates(inplace=True)    
    print("Количество пропусков в данных перед обработкой:")
    print(df.isnull().sum())  # Выводим количество пропусков в каждом столбце

    # Заполнение пропусков в полях с текстом
    df.fillna(value={
        'Description': 'Не указано',
        'Availability': 'Не указано',
        'Rating': 'Не указано'
    }, inplace=True)

    # Удаление строк с пропусками в цене
    df.dropna(subset=['Price_incl_tax'], inplace=True)

    print("Количество пропусков в данных после обработки:")
    print(df.isnull().sum())  # Выводим количество пропусков после обработки

    # Вычисляем общее количество книг
    total_books = df.shape[0]   
    print(f"Общее количество книг в собранных данных: {total_books}")

    # Выводим основные статистики по числовым данным
    print("Основные статистики по числовым данным:")
    print(df.describe())
    
    # Сохраняем данные в CSV
    df.to_csv('books_data.csv', index=False)
    print("Данные успешно собраны и сохранены в 'books_data.csv'.")

# Настраиваем автоматическую выгрузку данных по времени
schedule.every().day.at("03:57").do(fetch_books_data)

while True:
    schedule.run_pending()
    time.sleep(1)
   