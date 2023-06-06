import csv
import json

import psycopg2
import pymongo
import datetime
import redis

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


# authors
# returns dict(name, id)
def insert_authors(authors_s):
    print("Inserting authors to database.")
    dict1 = {}
    authors_s = {(s,) for s in authors_s}
    query = "INSERT INTO public.author(author_name) VALUES(%s)"
    cursor_obj.executemany(query, authors_s)
    con.commit()
    cursor_obj.execute("SELECT author_id, author_name from public.author")
    res = cursor_obj.fetchall()
    for r in res:
        dict1[r[1]] = r[0]

    print("Inserting authors completed.")
    return dict1


# categories
# returns dict(name, id)
def insert_categories(categories_s):
    print("Inserting categories to database.")
    dict1 = {}
    categories_s = {(s,) for s in categories_s}
    query = "INSERT INTO public.category(category_name) VALUES(%s)"
    cursor_obj.executemany(query, categories_s)
    con.commit()
    cursor_obj.execute("SELECT category_id, category_name from public.category")
    res = cursor_obj.fetchall()
    for r in res:
        dict1[r[1]] = r[0]

    print("Inserting categories completed.")
    return dict1


# publisher
# returns dict(name, id)
def insert_publishers(publishers_s):
    print("Inserting publishers to database.")
    dict1 = {}
    publishers_s = {(s,) for s in publishers_s}
    query = "INSERT INTO public.publisher(publisher_name) VALUES(%s)"
    cursor_obj.executemany(query, publishers_s)
    con.commit()
    cursor_obj.execute("SELECT publisher_id, publisher_name from public.publisher")
    res = cursor_obj.fetchall()
    for r in res:
        dict1[r[1]] = r[0]

    print("Inserting publishers completed.")
    return dict1


def insert_users(users_s):
    print("Inserting users to database.")
    dict1 = {}
    users_s = {(s,) for s in users_s}
    query = "INSERT INTO public.user(user_name) VALUES(%s)"
    cursor_obj.executemany(query, users_s)
    con.commit()
    cursor_obj.execute("SELECT user_id, user_name from public.user")
    res = cursor_obj.fetchall()
    for r in res:
        dict1[r[1]] = r[0]

    print("Inserting users completed.")
    return dict1


def insert_books(rows_p, authors_dict_p, categories_dict_p, publishers_dict_p):
    print("Inserting books to database.")
    query = "INSERT INTO public.book(title, description, image, preview_link, publisher_id, " \
            "published_date, info_link, rating_count) VALUES (%s,%s,%s,%s,%s,%s,%s,%s);"
    params = []
    for book_title in rows_p:
        book_row = rows_p[book_title]

        publisher_id = publishers_dict_p.get(book_row[5])

        date = book_row[6]
        if len(date) != 10:
            date = "1000-01-01"

        rating_count = book_row[9]
        if rating_count == '' or rating_count == ' ':
            rating_count = 0.0

        params.append((book_row[0], book_row[1], book_row[3], book_row[4],
                       publisher_id, date, book_row[7], rating_count))

    cursor_obj.executemany(query, params)
    con.commit()

    cursor_obj.execute("SELECT book_id, title from public.book")
    res = cursor_obj.fetchall()
    title_to_id = {}
    for r in res:
        title_to_id[r[1]] = r[0]

    for book_title in title_to_id:
        book_id = title_to_id[book_title]
        book_row = rows_p[book_title]

        book_authors = [] if len(book_row[2]) == 0 else eval(book_row[2])
        if " " in book_authors:
            book_authors.remove(" ")

        query = "INSERT INTO public.book_to_author(book_id, author_id) VALUES(%s, %s)"
        params = []
        for author in book_authors:
            author_id = authors_dict_p[author]
            params.append((book_id, author_id))

        cursor_obj.executemany(query, params)

        book_categories = [] if len(book_row[8]) == 0 else eval(book_row[8])
        if " " in book_categories:
            book_categories.remove(" ")

        query = "INSERT INTO public.book_to_category(book_id, category_id) VALUES(%s, %s)"
        params = []
        for category in book_categories:
            category_id = categories_dict_p[category]
            params.append((book_id, category_id))

        cursor_obj.executemany(query, params)

    con.commit()
    print("Inserting books completed.")
    return title_to_id


def insert_reviews(rows_p, users_dict_p, books_dict_p):
    print("Inserting reviews to database.")

    query = "INSERT INTO public.review(book_id, price, user_id, review_helpfulness, " \
            "review_score, review_time, review_summary, review_text) VALUES(%s,%s,%s,%s,%s,%s," \
            "%s,%s)"
    params = []

    line_counter = 0
    for row_p in rows_p:
        if line_counter == 0:
            line_counter += 1
        else:
            user_id = users_dict_p.get(row_p[4])
            book_id = books_dict_p[row_p[1]]

            print(row_p[7])
            date = datetime.datetime.fromtimestamp(0) if int(row_p[7]) < 1 else datetime.datetime.fromtimestamp(
                int(row_p[7]))

            params.append((book_id, row_p[2], user_id, row_p[5], row_p[6],
                           date, row_p[8], row_p[9]))

            # print(line_counter)
            if line_counter % 100000 == 0:
                print("Committing: {0}".format(line_counter))
                cursor_obj.executemany(query, params)
                con.commit()
                params.clear()

            line_counter += 1

    cursor_obj.executemany(query, params)
    con.commit()
    params.clear()
    print("Inserting reviews completed.")


def transfer_to_mongo_and_redis():
    print("Start transferring data to MongoDB and Redis.")
    booksCollection = mongo_db["books"]
    booksCollection.delete_many({})
    redis.flushdb()

    books_to_db = []

    cursor_obj.execute("SELECT * from public.book")
    books = cursor_obj.fetchall()

    cursor_obj.execute("SELECT user_id, user_name from public.user")
    res = cursor_obj.fetchall()
    user_dict = {}
    for r in res:
        user_dict[r[0]] = r[1]

    cursor_obj.execute("SELECT publisher_id, publisher_name from public.publisher")
    res = cursor_obj.fetchall()
    publishers_dict = {}
    for r in res:
        publishers_dict[r[0]] = r[1]

    cursor_obj.execute("SELECT * from public.review")
    res = cursor_obj.fetchall()
    reviews_by_book_dict = {}
    for r in res:
        user_name = user_dict.get(r[8])
        review_dict = {"price": r[2], "review_helpfulness": r[3], "review_summary": r[4],
                       "review_text": r[5],
                       "review_time": datetime.datetime(r[6].year, r[6].month, r[6].day),
                       "review_score": float(r[7]),
                       "user": user_name}

        value = reviews_by_book_dict.get(r[1])
        if value is None:
            revs = list()
            revs.append(review_dict)
            reviews_by_book_dict[r[1]] = revs
        else:
            value.append(review_dict)
            reviews_by_book_dict[r[1]] = value

    cnt = 0
    print("Start")
    for book in books:
        cnt += 1
        book_id = book[1]
        publisher = publishers_dict.get(book[6])

        query = "SELECT * FROM public.author WHERE author_id IN (SELECT author_id FROM public.book_to_author WHERE " \
                "book_id = %s)"
        cursor_obj.execute(query, (book_id,))
        authors12 = cursor_obj.fetchall()

        query = "SELECT * FROM public.category WHERE category_id IN (SELECT category_id FROM public.book_to_category " \
                "WHERE book_id = %s)"
        cursor_obj.execute(query, (book_id,))
        categories12 = cursor_obj.fetchall()

        book_to_db = {"title": book[0], "description": book[2], "image": book[3], "preview_link": book[4],
                      "info_link": book[5], "publisher": publisher, "rating_count": float(book[7]),
                      "published_date": datetime.datetime(book[8].year, book[8].month, book[8].day)}

        authors1 = []
        for author in authors12:
            authors1.append(author[1])
        book_to_db["authors"] = authors1

        categories1 = []
        for category in categories12:
            categories1.append(category[1])
        book_to_db["categories"] = categories1

        reviews = reviews_by_book_dict.get(book_id)
        if reviews is not None:
            book_to_db["reviews"] = reviews


        # TODO do urzytku gdy dane w postgres są całe z csv - pominie jedną książkę z dużą ilością reviews
        # try:
        #     booksCollection.insert_one(book_to_db)
        # except pymongo.errors.DocumentTooLarge as e:
        #     print(e)
        #     print("Error on book with id: {0}".format(book_id))
        #     print("That book has {0} reviews".format(len(book_to_db["reviews"])))

        #====================================
        books_to_db.append(book_to_db)

        #============================= REDIS SECTION===============================================
        redis.set(book_to_db["title"], json.dumps(book_to_db, default=str))

        # for field, value in book_to_db.items():
        #     if field == "reviews":
        #         continue
        #     elif field == "authors" or field == "categories" or field == "published_date":
        #         redis.hset(book_to_db["title"], field, str(value))
        #     else:
        #         redis.hset(book_to_db["title"], field, value)
        #
        # id = 1
        # revs = book_to_db.get("reviews")
        # if revs is not None:
        #     for rev in revs:
        #         for field, value in rev.items():
        #             if field == "review_time":
        #                 redis.hset(book_to_db["title"] + " R " + str(id), field, str(value))
        #             else:
        #                 redis.hset(book_to_db["title"] + " R " + str(id), field, value)
        #         redis.lpush("Reviews for " + book_to_db["title"], book_to_db["title"] + " R " + str(id))
        #         id += 1
        #============================= REDIS SECTION===============================================

        if cnt % 5000 == 0:
            print("Inserting {0} books".format(cnt))
            booksCollection.insert_many(books_to_db)
            books_to_db = []

    booksCollection.insert_many(books_to_db)

    print("Transferring to MongoDB and Redis completed.")

''' Books_data.csv
0 - title
1 - description
2 - list of authors *
3 - image
4 - preview_link
5 - publisher *
6 - published_date
7 - info_link
8 - list of categories *
9 - rating_count
'''

with open('books_data.csv', encoding="utf8") as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    authors_set = set()
    categories_set = set()
    publishers_set = set()
    rows = dict()

    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            print(f'Column names are: {", ".join(row)}')
            line_count += 1
        else:
            authors = [] if len(row[2]) == 0 else eval(row[2])
            authors_set.update(authors)

            categories = [] if len(row[8]) == 0 else eval(row[8])
            categories_set.update(categories)

            publishers_set.update([row[5]])

            rows[row[0]] = row
            line_count += 1

authors_set.discard(" ")
authors_dict = insert_authors(authors_set)

categories_set.discard(" ")
categories_dict = insert_categories(categories_set)

publishers_set.discard(" ")
publishers_dict = insert_publishers(publishers_set)

books_dict = insert_books(rows, authors_dict, categories_dict, publishers_dict)

''' Books_rating.csv
0 - id
1 - title
2 - price
3 - user_id
4 - profile_name
5 - review_helpfulness
6 - review_score
7 - review_time
8 - review_summary
9 - review_text
'''

with open('Books_rating.csv', encoding="utf8") as csv_file:
    csv_reader1 = csv.reader(csv_file, delimiter=',')

    users_set = set()
    rows = list()

    line_count = 0
    for row in csv_reader1:
        if line_count == 0:
            print(f'Column names are: {", ".join(row)}')
            line_count += 1
        else:
            users_set.update([row[4]])

            rows.append(row)
            line_count += 1

users_set.discard(" ")
users_dict = insert_users(users_set)
insert_reviews(rows, users_dict, books_dict)


transfer_to_mongo_and_redis()
redis.close()
mongoClient.close()
con.close()
print("Completed.")
