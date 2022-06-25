import pymysql

from configs.db_auth_data import host, port, user, password, db_name, table_name

matches = {
    "year": "Год выпуска",
    "last_name": "Фамилия автора",
    "fist_name": "Имя автора",
    "executor": "Исполнитель",
    "cycle_name": "Цикл/серия",
    "book_number": "Номер книги",
    "genre": "Жанр",
    "edition_type": "Тип издания",
    "category": "Категория",
    "audio_codec": "Аудиокодек",
    "bitrate": "Битрейт",
    "bitrate_type": "Вид битрейта",
    "sampling_frequency": "Частота дискретизации",
    "count_of_channels": "Количество каналов (моно-стерео)",
    "book_duration": "Время звучания",
    "description": "Описание",
}

columns = [
    "book_page_id",
    "url",
    "row_name",
    "book_name",
    "year",
    "last_name",
    "fist_name",
    "executor",
    "cycle_name",
    "book_number",
    "genre",
    "edition_type",
    "category",
    "audio_codec",
    "bitrate",
    "bitrate_type",
    "sampling_frequency",
    "count_of_channels",
    "book_duration",
    "description",
    "img_url",
    "magnet_link",
    "tor_size",
    "no_book"
]


def exec_wrapper(execute, *args, commit=True):
    conn = None
    try:
        if len(args) == 2:
            conn = args[0]
            args = args[1:]
        else:
            conn = create_conn()
        try:
            res = execute(conn, *args)
            if commit:
                conn.commit()
            else:
                return conn
            return res
        finally:
            if commit:
                conn.close()
            else:
                return conn
    except Exception as ex:
        if not commit and conn is not None:
            conn.close()
        return ex


def fix_query(query):
    query = add_semicolon(query.strip()).replace('\n', ' ')
    while '  ' in query:
        query = query.replace('  ', ' ')
    return query


def add_semicolon(query):
    if query[len(query) - 1] != ';':
        return query + ';'
    return query


def create_conn():
    return pymysql.connect(
        host=host,
        port=int(port),
        user=user,
        password=password,
        database=db_name,
        cursorclass=pymysql.cursors.DictCursor
    )


def exec_query(conn, query):
    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
        return result


def execute_query(query):
    return exec_wrapper(lambda c, q: exec_query(c, fix_query(q)), query)


def execute_queries(queries):
    results = []
    conn = None
    try:
        for query in queries:
            if conn is None:
                args = [query]
            else:
                args = [conn, query]
            result = exec_wrapper(lambda c, q: exec_query(c, fix_query(q)), *args, commit=False)
            results.append(result)
            if result is not None:
                if not isinstance(result, str):
                    conn = result
                else:
                    return result  # Exception
        conn.commit()
        return results
    except Exception as ex:
        return ex
    finally:
        conn.close()


def json_to_db(data):
    def get_value(book_data, key):
        def get_sub_val():
            if key == "book_page_id":
                if book_data.get(key) is None:
                    if book_data.get('url') is not None:
                        return book_data['url'].split("t=")[1]
                    else:
                        return ""
                else:
                    return book_data[key]
            elif book_data.get(key) is None:
                return ""
            elif key == "no_book":
                return int(book_data[key])
            return book_data[key]

        def add_quotes(value):
            if key == "no_book":
                return value
            return f"'{value}'"

        return add_quotes(get_sub_val())

    def get_values(book_data):
        return f"""(
           default,
           {",".join(map(lambda col_name: f"{get_value(book_data, col_name)}", columns))}
        )"""

    columns_for_insert = f"id,{','.join(columns)}"
    values = map(get_values, data)
    query = f"INSERT INTO {table_name} ({columns_for_insert}) VALUES " + ",".join(values)
    # print(query)
    return execute_query(query)


def additional_data_to_db(data):

    def get_sql_value(book_data, key):
        if key in book_data:
            if key == "no_book":
                value = book_data[key]
            else:
                value = f"'{book_data[key]}'"
        else:
            value = "null"
        return value

    def get_values(book_data):
        return f"""
           {get_sql_value(book_data, 'id')}, 
           {get_sql_value(book_data, 'img_url')},
           {get_sql_value(book_data, 'magnet_link')},
           {get_sql_value(book_data, 'tor_size')},
           {get_sql_value(book_data, 'no_book')}
        """

    values = "),(".join(map(get_values, data))
    query = f"""INSERT INTO {table_name}
                    (id, img_url, magnet_link, tor_size, no_book) 
                VALUES  ({values})
                ON DUPLICATE KEY UPDATE 
                    img_url = VALUES(img_url),
                    magnet_link = VALUES(magnet_link),
                    tor_size = VALUES(tor_size),
                    no_book = VALUES(no_book);
            """
    return execute_query(query)


def get_book_page_ids():
    result = execute_query(f"SELECT book_page_id FROM {table_name} ORDER BY book_page_id DESC;")
    return list(map(lambda it: it['book_page_id'], result))


def get_update_book_data_query(row_id, par_name, par_value):
    query = f"""
        UPDATE {table_name}
        SET {par_name} = {par_value}
        WHERE id = {row_id}
    """
    # print(query)
    return query


def get_ids():
    return list(map(
        lambda data: data['book_page_id'],
        execute_query(f"""
            SELECT book_page_id
            FROM rutracker_books;
        """)
    ))
