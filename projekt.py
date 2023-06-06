from tkinter import *
import psycopg2
import pymongo
import redis
import json
import time
import matplotlib.pyplot as plt
import numpy as np

con = psycopg2.connect(
    database="gaweckim",
    user="gaweckim",
    password="1ACGYkLegL",
    host="localhost",
    port='5432'
)
cursor_obj = con.cursor()

mongoClient = pymongo.MongoClient()
mongo_db = mongoClient["mongoZTBD"]

redis = redis.Redis(host="localhost", port=6379)

postgresql_string = "PostgreSQL: "
mongo_string = "MongoDB: "
redis_string = "Redis: "


def modify_times(postgresql_time, mongo_time, redis_time):
    label_postgresql.configure(text=postgresql_string + str(postgresql_time))
    label_mongoDB.configure(text=mongo_string + str(mongo_time))
    label_redis.configure(text=redis_string + str(redis_time))


# TODO to zapytanie powinno zaciągnąć wszstko ale dostaję error że skończyła się pamięć, wobiec tego zrobię osobne
#  zaciągnięcia danych (kilka selectów)
'''
select title, description, image, preview_link, info_link, publisher_name, rating_count, published_date, 
ARRAY_AGG (DISTINCT author.author_name) authors,
ARRAY_AGG (DISTINCT category.category_name) categories,
ARRAY_AGG (ROW (price, review_helpfulness, review_summary, review_text, review_time, review_score, user_name)) reviews
from public.book 
join public.book_to_author ON book_to_author.book_id = book.book_id join public.author ON author.author_id = book_to_author.author_id 
join public.publisher ON publisher.publisher_id = book.publisher_id
join public.book_to_category ON book_to_category.book_id = book.book_id join public.category ON category.category_id = book_to_category.category_id
join public.review ON review.book_id = book.book_id
join public.user on public.user.user_id = review.user_id
group by title,description,image,preview_link, info_link, publisher_name, rating_count, published_date
'''


def get_all_data():
    query1 = '''
    select title, ARRAY_AGG (author.author_name) authors
    from public.book 
    join public.book_to_author ON book_to_author.book_id = book.book_id 
        join public.author ON author.author_id = book_to_author.author_id 
    GROUP BY title
    '''

    query2 = '''
    select title, ARRAY_AGG (category.category_name) categories
    from public.book
    join public.book_to_category ON book_to_category.book_id = book.book_id 
    join public.category ON category.category_id = book_to_category.category_id
    GROUP BY title
    '''

    # TODO ta jest wolniejsza ale zwaraca jeden wiersz dla danej książki
    # query3 = '''
    # select title, ARRAY_AGG (ROW (price, review_helpfulness, review_summary, review_text, review_time, review_score, user_name)) reviews
    # from public.book
    # join public.review ON review.book_id = book.book_id
    #     join public.user on public.user.user_id = review.user_id
    # GROUP BY title
    # '''

    # TODO ta jest szybsza
    query3 = '''
    select title, price, review_helpfulness, review_summary, review_text, review_time, review_score, user_name
    from public.book
    join public.review ON review.book_id = book.book_id
        join public.user on public.user.user_id = review.user_id
    order by title
    '''

    query4 = '''
    select title, description, image, preview_link, info_link, publisher_name, rating_count, published_date
    from public.book 
    join public.publisher ON publisher.publisher_id = book.publisher_id
    '''

    postgres_start = time.perf_counter()  # TODO umieszczenie czasu tutaj skraca czas bo liczy tylko zapytania a nie budowanie stringa, tak myśle
    cursor_obj.execute(query1)
    cursor_obj.execute(query2)
    cursor_obj.execute(query3)
    cursor_obj.execute(query4)
    postgres_end = time.perf_counter()

    mongo_start = time.perf_counter()
    booksCollection = mongo_db["books"]
    booksCollection.find()
    mongo_end = time.perf_counter()

    redis_start = time.perf_counter()
    keys = redis.keys("*")
    books = []
    for key in keys:
        books.append(redis.get(key))
    redis_end = time.perf_counter()

    modify_times(round(postgres_end - postgres_start, 6), round(mongo_end - mongo_start, 6),
                 round(redis_end - redis_start, 6))


def get_by_title():
    title = title_entry.get()

    query = '''
    select title, description, image, preview_link, info_link, publisher_name, rating_count, published_date, 
    ARRAY_AGG (DISTINCT author.author_name) authors,
    ARRAY_AGG (DISTINCT category.category_name) categories,
    ARRAY_AGG (ROW (price, review_helpfulness, review_summary, review_text, review_time, review_score, user_name)) reviews
    from public.book 
    join public.book_to_author ON book_to_author.book_id = book.book_id join public.author ON author.author_id = book_to_author.author_id 
    join public.publisher ON publisher.publisher_id = book.publisher_id
    join public.book_to_category ON book_to_category.book_id = book.book_id join public.category ON category.category_id = book_to_category.category_id
    join public.review ON review.book_id = book.book_id
    join public.user on public.user.user_id = review.user_id
    where title=%s
    group by title,description,image,preview_link, info_link, publisher_name, rating_count, published_date
    '''

    postgres_start = time.perf_counter()  # TODO umieszczenie czasu tutaj skraca czas bo liczy tylko zapytania a nie budowanie stringa, tak myśle
    cursor_obj.execute(query, (title,))
    postgres_end = time.perf_counter()

    query2 = '''
        select title, description, image, preview_link, info_link, rating_count, published_date
        from public.book 
        where title=%s
        '''

    postgres_start2 = time.perf_counter()
    cursor_obj.execute(query2, (title,))
    postgres_end2 = time.perf_counter()

    mongo_start = time.perf_counter()
    booksCollection = mongo_db["books"]
    booksCollection.find({"title": title})
    mongo_end = time.perf_counter()

    redis_start = time.perf_counter()
    book_redis = redis.get(title)
    redis_end = time.perf_counter()

    modify_times(str(round(postgres_end - postgres_start, 6)) + " / " + str(round(postgres_end2 - postgres_start2, 6)),
                 round(mongo_end - mongo_start, 6),
                 round(redis_end - redis_start, 6))


def delete_by_title():
    title = title_entry.get()

    query_find_book = "select * from public.book where title=%s"
    query_find_book_authors = "select * from public.book_to_author where book_id=%s"
    query_find_book_categories = "select * from public.book_to_category where book_id=%s"
    query_find_book_reviews = "select * from public.review where book_id=%s"
    query_delete = "delete from public.book where title=%s"
    query_insert = '''
        insert into public.book(title,description,image,preview_link,info_link,publisher_id,
        rating_count,published_date) 
        values(%s,%s,%s,%s,%s,%s,%s,%s)
        returning book_id
        '''
    query_insert_authors = "insert into public.book_to_author(book_id, author_id) VALUES(%s,%s)"
    query_insert_categories = "insert into public.book_to_category(book_id, category_id) VALUES(%s,%s)"
    query_insert_reviews = "insert into public.review(book_id, price, review_helpfulness, review_summary, " \
                           "review_text, review_time, review_score, user_id) values(%s,%s,%s,%s,%s,%s,%s,%s)"

    cursor_obj.execute(query_find_book, (title,))
    book = cursor_obj.fetchone()
    book_id = book[1]
    cursor_obj.execute(query_find_book_authors, (book_id,))
    res = cursor_obj.fetchall()
    authors = []  # list of authors id's
    for r in res:
        authors.append(r[1])
    cursor_obj.execute(query_find_book_categories, (book_id,))
    res = cursor_obj.fetchall()
    categories = []  # list of categories id's
    for r in res:
        categories.append(r[1])
    cursor_obj.execute(query_find_book_reviews, (book_id,))
    reviews = cursor_obj.fetchall()

    postgres_start = time.perf_counter()
    cursor_obj.execute(query_delete, (title,))
    con.commit()
    postgres_end = time.perf_counter()

    # insert book
    # get new id
    postgres_start2 = time.perf_counter()
    cursor_obj.execute(query_insert, (book[0], book[2], book[3], book[4], book[5], book[6], book[7], book[8]))
    con.commit()
    new_book_id = cursor_obj.fetchone()[0]

    # insert book_to_authors
    book_to_authors_list = []
    for author_id in authors:
        book_to_authors_list.append((new_book_id, author_id,))
    cursor_obj.executemany(query_insert_authors, book_to_authors_list)

    # insert book_to_category
    book_to_categories_list = []
    for category_id in categories:
        book_to_categories_list.append((new_book_id, category_id))
    cursor_obj.executemany(query_insert_categories,
                           book_to_categories_list)

    # insert reviews
    new_reviews = []
    for review in reviews:
        new_reviews.append((new_book_id, review[2], review[3], review[4], review[5], review[6], review[7], review[8]))
    cursor_obj.executemany(query_insert_reviews, new_reviews)
    con.commit()
    postgres_end2 = time.perf_counter()

    booksCollection = mongo_db["books"]
    book_from_mongo = booksCollection.find_one({"title": title})
    mongo_start = time.perf_counter()
    booksCollection.delete_one({"title": title})
    mongo_end = time.perf_counter()
    mongo_start2 = time.perf_counter()
    booksCollection.insert_one(book_from_mongo)
    mongo_end2 = time.perf_counter()

    print("Redis")
    book = redis.get(title)
    redis_start = time.perf_counter()
    redis.delete(title)
    redis_end = time.perf_counter()

    redis_start2 = time.perf_counter()
    redis.set(title, book)
    redis_end2 = time.perf_counter()

    modify_times(str(round(postgres_end - postgres_start, 6)) + " / " + str(round(postgres_end2 - postgres_start2, 6)),
                 str(round(mongo_end - mongo_start, 6)) + " / " + str(round(mongo_end2 - mongo_start2, 6)),
                 str(round(redis_end - redis_start, 6)) + " / " + str(round(redis_end2 - redis_start2, 6)))


def charts():
    input_window = Toplevel(app)
    input_window.title("Wprowadź wartości")

    def calculate_avg_for_books(titles):
        times_postgres = []
        times_mongo = []
        times_redis = []
        result_postgres = []
        result_mongo = []
        result_redis = []

        for title in titles:
            query = "select AVG(review_score) from review join book ON book.book_id = review.book_id where title=%s"
            postgres_start = time.perf_counter()
            cursor_obj.execute(query, (title,))
            postgres_end = time.perf_counter()
            res = cursor_obj.fetchone()
            if res[0] is not None:
                result_postgres.append(str(round(res[0], 1)))
            else:
                result_postgres.append("Avg not fount")

            pipeline = [
                {
                    '$match': {
                        'title': title
                    }
                },
                {
                    '$unwind': '$reviews'
                },
                {
                    '$group': {
                        '_id': None,
                        'average_score': {
                            '$avg': '$reviews.review_score'
                        }
                    }
                }
            ]

            booksCollection = mongo_db["books"]

            mongo_start = time.perf_counter()
            x = list(booksCollection.aggregate(pipeline))
            mongo_end = time.perf_counter()
            if len(x) != 0:
                result_mongo.append(str(round(x[0]['average_score'], 1)))
            else:
                result_mongo.append("Avg not fount")

            redis_start = time.perf_counter()
            result = 0
            book_json_str = redis.get(title)
            if book_json_str is not None:
                book = json.loads(book_json_str)
                sum = 0
                cnt = 0
                for rev in book.get("reviews"):
                    sum = sum + rev["review_score"]
                    cnt = cnt + 1
                result = round(sum / cnt, 1)
            else:
                result = "Avg not fount"

            redis_end = time.perf_counter()
            result_redis.append(str(result))

            times_postgres.append(postgres_end - postgres_start)
            times_mongo.append(mongo_end - mongo_start)
            times_redis.append(redis_end - redis_start)

        return times_postgres, times_mongo, times_redis, result_postgres, result_mongo, result_redis

    def calculate_cnt_reviews_for_books(titles):
        times_postgres = []
        times_mongo = []
        times_redis = []
        result_postgres = []
        result_mongo = []
        result_redis = []

        for title in titles:
            query = "select COUNT(*) from review join book ON book.book_id = review.book_id where title=%s"
            postgres_start = time.perf_counter()
            cursor_obj.execute(query, (title,))
            postgres_end = time.perf_counter()
            res = cursor_obj.fetchone()
            if res[0] is not None:
                result_postgres.append(str(round(res[0], 1)))
            else:
                result_postgres.append("COUNT not fount")

            pipeline = [
                {
                    '$match': {
                        'title': title
                    }
                },
                {
                    '$unwind': '$reviews'
                },
                {
                    '$group': {
                        '_id': None,
                        'reviews_count': {'$sum': 1}
                    }
                }
            ]

            booksCollection = mongo_db["books"]

            mongo_start = time.perf_counter()
            x = list(booksCollection.aggregate(pipeline))
            mongo_end = time.perf_counter()
            if len(x) != 0:
                result_mongo.append(str(round(x[0]['reviews_count'], 1)))
            else:
                result_mongo.append("COUNT not fount")

            redis_start = time.perf_counter()
            result = 0
            book_json_str = redis.get(title)
            if book_json_str is not None:
                book = json.loads(book_json_str)
                result = len(book["reviews"])
            else:
                result = "COUNT not fount"

            redis_end = time.perf_counter()
            result_redis.append(str(result))

            times_postgres.append(postgres_end - postgres_start)
            times_mongo.append(mongo_end - mongo_start)
            times_redis.append(redis_end - redis_start)

        return times_postgres, times_mongo, times_redis, result_postgres, result_mongo, result_redis

    def print_chart(labels, toggle):
        plt.close()
        widgets = input_window.winfo_children()
        for widget in widgets:
            if isinstance(widget, Label):
                widget.destroy()

        if toggle:
            values_A, values_B, values_C, res_postgres, res_mongo, res_redis = calculate_avg_for_books(labels)
        else:
            values_A, values_B, values_C, res_postgres, res_mongo, res_redis = calculate_cnt_reviews_for_books(labels)

        Label(input_window, text=labels[0] + ": ").pack()
        Label(input_window, text="PostgresSQL output: " + res_postgres[0]).pack()
        Label(input_window, text="Mongo output: " + res_mongo[0]).pack()
        Label(input_window, text="Redis output: " + res_redis[0]).pack()
        Label(input_window, text=labels[1] + ": ").pack()
        Label(input_window, text="PostgresSQL output: " + res_postgres[1]).pack()
        Label(input_window, text="Mongo output: " + res_mongo[1]).pack()
        Label(input_window, text="Redis output: " + res_redis[1]).pack()
        Label(input_window, text=labels[2] + ": ").pack()
        Label(input_window, text="PostgresSQL output: " + res_postgres[2]).pack()
        Label(input_window, text="Mongo output: " + res_mongo[2]).pack()
        Label(input_window, text="Redis output: " + res_redis[2]).pack()
        Label(input_window, text=labels[3] + ": ").pack()
        Label(input_window, text="PostgresSQL output: " + res_postgres[3]).pack()
        Label(input_window, text="Mongo output: " + res_mongo[3]).pack()
        Label(input_window, text="Redis output: " + res_redis[3]).pack()

        width = 0.15
        index = np.arange(len(labels))
        plt.bar(index - width, values_A, width, label="PostgeSQL")
        plt.bar(index, values_B, width, label="MongoDB")
        plt.bar(index + width, values_C, width, label="Redis")
        plt.xticks(index, labels, rotation=10)

        plt.legend()
        if toggle:
            plt.title("AVG rating count for 4 books")
        else:
            plt.title("Count reviews for 4 books")
        plt.show()

    def check_inputs():
        input1 = input1_entry.get()
        input2 = input2_entry.get()
        input3 = input3_entry.get()
        input4 = input4_entry.get()

        if input1 and input2 and input3 and input4:
            return True

    def handle_avg():
        if check_inputs():
            input1 = input1_entry.get()
            input2 = input2_entry.get()
            input3 = input3_entry.get()
            input4 = input4_entry.get()
            print_chart((input1, input2, input3, input4), True)

    def handle_cnt():
        if check_inputs():
            input1 = input1_entry.get()
            input2 = input2_entry.get()
            input3 = input3_entry.get()
            input4 = input4_entry.get()
            print_chart((input1, input2, input3, input4), False)


    input1_label = Label(input_window, text="Input 1:")
    input1_label.pack()
    input1_entry = Entry(input_window)
    input1_entry.pack()

    input2_label = Label(input_window, text="Input 2:")
    input2_label.pack()
    input2_entry = Entry(input_window)
    input2_entry.pack()

    input3_label = Label(input_window, text="Input 3:")
    input3_label.pack()
    input3_entry = Entry(input_window)
    input3_entry.pack()

    input4_label = Label(input_window, text="Input 4:")
    input4_label.pack()
    input4_entry = Entry(input_window)
    input4_entry.pack()

    confirm_button = Button(input_window, text="Calculate avg rating for book", command=handle_avg)
    confirm_button.pack()
    confirm_button2 = Button(input_window, text="Calculate reviews count for book", command=handle_cnt)
    confirm_button2.pack()


app = Tk()
app.title("ZTBD Project")
app.geometry("700x350")
Button(app, text="Get all data", width=16, command=get_all_data).grid(row=0, column=0, padx=5, pady=5)
Button(app, text="Select book by title", width=20, command=get_by_title).grid(row=0, column=2, padx=5, pady=5)
Button(app, text="Delete/Insert book by title", width=20, command=delete_by_title).grid(row=1, column=2, padx=5, pady=5)
Label(app, text="Book title:").grid(row=0, column=1, padx=5, pady=5)
title_entry = Entry(app)
title_entry.grid(row=1, column=1, padx=5, pady=5)

Label(app, text="Execution time in seconds for:").grid(row=2, padx=5, pady=5)
label_postgresql = Label(app, text=postgresql_string)
label_postgresql.grid(row=3, column=0, padx=5, pady=5)
label_mongoDB = Label(app, text=mongo_string)
label_mongoDB.grid(row=4, column=0, padx=5, pady=5)
label_redis = Label(app, text=redis_string)
label_redis.grid(row=5, column=0, padx=5, pady=5)

Button(app, text="Charts", width=20, command=charts).grid(row=3, column=2, padx=5, pady=5)

app.mainloop()
