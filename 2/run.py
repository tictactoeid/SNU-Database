#import pymysql
import numpy as np
import pandas as pd
import mysql.connector

connection = mysql.connector.connect(
    host='astronaut.snu.ac.kr',
    port=7000,
    user='DB2020_15127',
    password='DB2020_15127',
    db='DB2020_15127',
    charset='utf8'
)

DEBUG = True # TODO: make it False
# TODO: connection.commit()

DIRECTOR_TABLE_CREATE = "create table director ( name varchar(255), primary key (name) );"
CUSTOMER_TABLE_CREATE = "create table customer ( id int auto_increment, name varchar(255), age int, class varchar(255), primary key (id) );"
MOVIE_TABLE_CREATE = "create table movie ( id int auto_increment, title varchar(255), director varchar(255), price int, primary key (id), foreign key (director) references director (name) );"
MOVIECUSTOMER_TABLE_CREATE = "create table moviecustomer ( movie_id int, customer_id int, score int, reserve_price int, foreign key (customer_id) references customer (id), foreign key (movie_id) references movie (id) );"

# TODO: moviecustomer의 reserve column 삭제
# TODO: row가 존재하는 것 자체로 예매한 것으로 간주할 것

def get_reserve_price(price, class_):
    class_ = class_.lower()
    if class_ == "basic":
        reserve_price = price
    elif class_ == "premium":
        reserve_price = int(price * 0.75)
    elif class_ == "vip":
        reserve_price = int(price * 0.5)
    else:
        raise ValueError
    return reserve_price

# Problem 1 (5 pt.)
def initialize_database():
    # YOUR CODE GOES HERE
    # TODO: ID
    with connection.cursor(dictionary=True, buffered=True) as cursor:
        cursor.execute("show tables like 'director';")
        result = cursor.fetchall()
        if len(result) == 0:
            cursor.execute(DIRECTOR_TABLE_CREATE)

        cursor.execute("show tables like 'customer';")
        result = cursor.fetchall()
        if len(result) == 0:
            cursor.execute(CUSTOMER_TABLE_CREATE)

        cursor.execute("show tables like 'movie';")
        result = cursor.fetchall()
        if len(result) == 0:
            cursor.execute(MOVIE_TABLE_CREATE)

        cursor.execute("show tables like 'moviecustomer';")
        result = cursor.fetchall()
        if len(result) == 0:
            cursor.execute(MOVIECUSTOMER_TABLE_CREATE)

        connection.commit()

        data = pd.read_csv("./data.csv")
        for row in data.iterrows():
            if DEBUG:
                #print(row)
                #print(type(row))
                #print(row[1])
                #print(row[1]["title"])
                pass
            title = row[1]["title"]
            director = row[1]["director"]
            price = row[1]["price"]
            name = row[1]["name"]
            age = row[1]["age"]
            class_ = row[1]["class"].lower()

            #sql = "insert into director values (%s);"
            #val = (director,)
            #cursor.execute(sql, val)

            # TODO: invalid row시 전부 취소하게
            # TODO: error msg 띄워야 함?
            # insert 전 예외 처리 먼저
            if type(price) != int or price < 0 or price > 100000:
                #print('Movie price should be from 0 to 100000')
                continue
            if type(age) != int or age < 12 or age > 110:
                #print('User age should be from 12 to 110')
                continue
            if class_ not in ["basic", "premium", "vip"]:
                #print('User class should be basic, premium or vip')
                continue

            # inserting movie
            # (제목, 감독이름)이 같은 경우 같은 영화로 판단
            # 제목은 같으나, 감독이름이 다른 경우 잘못된 input으로 판단, 해당 row 전체를 무시
            # user의 경우 이름은 같고 나이가 다른 경우는 없다고 명시되어 있으나, 영화는 그렇지 않아 임의로 가정하였습니다
            with connection.cursor(dictionary=True, buffered=True) as cursor:
                # insert director
                cursor.execute("select * from director where name = %s;", (director,))
                cnt = cursor.rowcount
                if cnt == 0:
                    cursor.execute("insert into director values (%s);", (director,))

                # insert movie
                cursor.execute("select * from movie where title = %s;", (title,))
                cnt = cursor.rowcount
                if cnt != 0:
                    #print(f'Movie {title} already exists')
                    result = cursor.fetchone()
                    if DEBUG:
                        #print(result)
                        #print(type(result))
                        pass
                    if director != result["director"]:
                        continue
                    movie_id = result["id"]
                else:

                    cursor.execute("insert into movie (title, director, price) values (%s, %s, %s);", (title, director, price))
                    movie_id = cursor.lastrowid

                # insert customer
                cursor.execute("select * from customer where name = %s and age =  %s;", (name, age))
                cnt = cursor.rowcount
                if cnt != 0:
                    # 이미 존재하는 customer
                    # 한 customer가 여러 영화 볼 수도 있으므로, error 처리하지는 않음
                    result = cursor.fetchone()
                    user_id = result["id"]
                else:
                    # 새로운 user
                    cursor.execute("insert into customer (name, age, class) values (%s, %s, %s);", (name, age, class_))
                    user_id = cursor.lastrowid

                # 예매
                cursor.execute("select * from moviecustomer where movie_id = %s;", (movie_id,))

                cnt = cursor.rowcount
                if cnt >= 10:
                    #print(f'Movie {movie_id} has already been fully booked')
                    continue
                    # 해당 row 예외 처리
                    # commit()하지 않았으므로 앞선 insert는 반영되지 않고, 해당 row 자체를 건너뜀
                    # TODO: 앞선 insert 정말 반영되지 않았는지 확인
                reserve_price = get_reserve_price(price, class_)

                cursor.execute("select * from moviecustomer where movie_id = %s and customer_id = %s;",
                               (movie_id, user_id))
                cnt = cursor.rowcount
                if cnt != 0:
                    # row exists
                    continue
                else:
                    # row not exists
                    cursor.execute("insert into moviecustomer values (%s, %s, %s, %s);",
                                   (movie_id, user_id, None, reserve_price))

                # 모든 예외 처리 끝났으므로 commit
                connection.commit()

    print('Database successfully initialized')
    # YOUR CODE GOES HERE


# Problem 15 (5 pt.)
def reset():
    # TODO: 삭제 실시 전 확인 메시지를 띄우고 사용자 입력(y/n)을 받아야 한다
    # YOUR CODE GOES HERE
    with connection.cursor(dictionary=True) as cursor:
        # TODO: 순서 중요 - foreign key constraint
        cursor.execute("show tables like 'moviecustomer';")
        result = cursor.fetchall()
        if len(result) != 0:
            cursor.execute("drop table moviecustomer;")

        cursor.execute("show tables like 'movie';")
        result = cursor.fetchall()
        if len(result) != 0:
            cursor.execute("drop table movie;")

        cursor.execute("show tables like 'director';")
        result = cursor.fetchall()
        if len(result) != 0:
            cursor.execute("drop table director;")

        cursor.execute("show tables like 'customer';")
        result = cursor.fetchall()
        if len(result) != 0:
            cursor.execute("drop table customer;")

        connection.commit()
        if DEBUG:
            print("table dropped")

    initialize_database()
    # TODO
    # YOUR CODE GOES HERE


# Problem 2 (4 pt.)
def print_movies():
    # YOUR CODE GOES HERE
    with connection.cursor(dictionary=True, buffered=True) as cursor:
        cursor.execute("select id, title, director, price, avg(reserve_price), \
                       count(customer_id), avg(score) \
                       from movie left outer join moviecustomer on movie.id = moviecustomer.movie_id \
                       group by id \
                       order by id asc;") # moviecustomer에 row가 없을 수도 있으므로 left outer join을 해야 함.

        result = cursor.fetchall()
        print_col_names = ("id", "title", "director", "price", "avg. price", "reservation", "avg. rating")

        print('-' * 100)
        strFormat = "%-4s%-27s%-27s%-6s%-12s%-12s%-12s"
        print(strFormat % print_col_names)
        print('-' * 100)
        for row in result:
            if row["avg(reserve_price)"] is None:
                avg_reserve_price = "None"
            else:
                avg_reserve_price = round(row["avg(reserve_price)"], 2)
            if row["avg(score)"] is None:
                avg_score = "None"
            else:
                avg_score = round(row["avg(score)"], 2)
            # 평점이 존재하지 않는 경우 None으로 출력
            # 예매 가격도 마찬가지라고 가정.
            print_values = (row["id"], row["title"][0:26], row["director"][0:26], row["price"], avg_reserve_price, row["count(customer_id)"], avg_score)
            print(strFormat % print_values) # 너무 길어서 적당히 잘라서 출력
        print('-' * 100)

    # YOUR CODE GOES HERE

# Problem 3 (3 pt.)
def print_users():
    # YOUR CODE GOES HERE
    with connection.cursor(dictionary=True, buffered=True) as cursor:
        cursor.execute("select id, name, age, class from customer order by id asc;")
        result = cursor.fetchall()

        print_col_names = ('id', 'name', 'age', 'class')
        print('-' * 60)
        strFormat = "%-4s%-36s%-10s%-10s"
        print(strFormat % print_col_names)
        print('-' * 60)
        for row in result:
            print_values = (row["id"], row["name"][0:35], row["age"], row["class"])
            print(strFormat % print_values)
        print('-' * 60)

    # YOUR CODE GOES HERE


# Problem 4 (4 pt.)
def insert_movie():
    # YOUR CODE GOES HERE
    title = input('Movie title: ')
    director = input('Movie director: ')
    price = int(input('Movie price: ')) # 무조건 정수 입력된다고 가정 - qna로부터
    if price < 0 or price > 100000:
        print('Movie price should be from 0 to 100000')
        if DEBUG:
            print(type(price))
        return

    with connection.cursor(dictionary=True, buffered=True) as cursor:
        cursor.execute("select * from movie where title = %s;", (title,))
        cnt = cursor.rowcount
        if cnt != 0:
            print(f'Movie {title} already exists')
            return

        cursor.execute("select * from director where name = %s;", (director,))
        if cursor.rowcount == 0:
            cursor.execute("insert into director values (%s);", (director,))

        cursor.execute("insert into movie (title, director, price) values (%s, %s, %s);", (title, director, price))
        connection.commit()
        print('One movie successfully inserted')


# Problem 6 (4 pt.)
def remove_movie():
    # YOUR CODE GOES HERE
    movie_id = int(input('Movie ID: '))
    with connection.cursor(dictionary=True, buffered=True) as cursor:
        cursor.execute("select * from movie where id = %s;", (movie_id,))
        cnt = cursor.rowcount
        if cnt == 0:
            print(f'Movie {movie_id} does not exist')
            return
        cursor.execute("delete from moviecustomer where movie_id = %s;", (movie_id,))
        cursor.execute("delete from movie where id = %s;", (movie_id,))
        connection.commit()
        print('One movie successfully removed')

# Problem 5 (4 pt.)
def insert_user():
    # YOUR CODE GOES HERE
    name = input('User name: ')
    age = int(input('User age: '))
    class_ = input('User class: ').lower() # name, title 등은 대소문자를 구분하나, class는 구분하지 않는다고 가정
    if age < 12 or age > 110:
        print('User age should be from 12 to 110')
        return
    if class_ not in ["basic", "premium", "vip"]:
        print('User class should be basic, premium or vip')
        return

    with connection.cursor(dictionary=True, buffered=True) as cursor:
        cursor.execute("select * from customer where name = %s and age =  %s;", (name, age))
        cnt = cursor.rowcount
        if cnt != 0:
            print(f'The user ({name}, {age}) already exists')
            return

        cursor.execute("insert into customer (name, age, class) values (%s, %s, %s);", (name, age, class_))
        connection.commit()
        print('One user successfully inserted')

# Problem 7 (4 pt.)
def remove_user():
    # YOUR CODE GOES HERE
    user_id = int(input('User ID: '))
    with connection.cursor(dictionary=True, buffered=True) as cursor:
        cursor.execute("select * from customer where id = %s;", (user_id,))
        cnt = cursor.rowcount
        if cnt == 0:
            print(f'User {user_id} does not exist')
            return
        cursor.execute("delete from moviecustomer where customer_id = %s;", (user_id,))
        cursor.execute("delete from customer where id = %s;", (user_id,))
        connection.commit()
        print('One user successfully removed')
    # TODO: delete 평점

# Problem 8 (5 pt.)
def book_movie():
    # YOUR CODE GOES HERE
    movie_id = int(input('Movie ID: '))
    user_id = int(input('User ID: '))

    # error message
    with connection.cursor(dictionary=True, buffered=True) as cursor:
        cursor.execute("select * from movie where id = %s;", (movie_id,))
        cnt = cursor.rowcount
        if cnt == 0:
            print(f'Movie {movie_id} does not exist')
            return
        result = cursor.fetchone()
        price = result["price"]

        cursor.execute("select * from customer where id = %s;", (user_id,))
        cnt = cursor.rowcount
        if cnt == 0:
            print(f'User {user_id} does not exist')
            return
        result = cursor.fetchone()
        class_ = result["class"]

        cursor.execute("select * from moviecustomer where movie_id = %s;", (movie_id,))
        cnt = cursor.rowcount
        if cnt >= 10:
            print(f'Movie {movie_id} has already been fully booked')
            return
        reserve_price = get_reserve_price(price, class_)

        cursor.execute("select * from moviecustomer where movie_id = %s and customer_id = %s;", (movie_id, user_id))
        cnt = cursor.rowcount
        if cnt != 0:
            # row exists
            print(f'User {user_id} already booked movie {movie_id}')
            return

        #cursor.execute("update moviecustomer set reserve_price = %s where movie_id = %s and customer_id = %s;", (reserve_price, movie_id, user_id))

        else:
            # row not exists
            # not reserved yet
            cursor.execute("insert into moviecustomer values (%s, %s, %s, %s);", (movie_id, user_id, None, reserve_price))
    connection.commit()
    print('Movie successfully booked')
    # YOUR CODE GOES HERE

# Problem 9 (5 pt.)
def rate_movie():
    # YOUR CODE GOES HERE
    movie_id = int(input('Movie ID: '))
    user_id = int(input('User ID: '))
    rating = int(input('Ratings (1~5): '))
    if rating not in [1, 2, 3, 4, 5]:
        print(f'Wrong value for a rating')
        return

    with connection.cursor(dictionary=True, buffered=True) as cursor:
        cursor.execute("select * from movie where id = %s;", (movie_id,))
        cnt = cursor.rowcount
        if cnt == 0:
            print(f'Movie {movie_id} does not exist')
            return

        cursor.execute("select * from customer where id = %s;", (user_id,))
        cnt = cursor.rowcount
        if cnt == 0:
            print(f'User {user_id} does not exist')
            return

        cursor.execute("select * from moviecustomer where movie_id = %s and customer_id = %s;", (movie_id, user_id))
        result = cursor.fetchone()
        if not result:
            print(f'User {user_id} has not booked movie {movie_id} yet')
            return
        if result["score"]:
            print(f'User {user_id} has already rated movie {movie_id}')
            return

        cursor.execute("update moviecustomer set score = %s where movie_id = %s and customer_id = %s;", (rating, movie_id, user_id))

    # success message
    connection.commit()
    print('Movie successfully rated')
    # YOUR CODE GOES HERE

# Problem 10 (5 pt.)
def print_users_for_movie():
    # YOUR CODE GOES HERE
    movie_id = int(input('Movie ID: '))
    with connection.cursor(dictionary=True, buffered=True) as cursor:
        cursor.execute("select * from movie where id = %s;", (movie_id,))
        cnt = cursor.rowcount
        if cnt == 0:
            print(f'Movie {movie_id} does not exist')
            return

        # TODO: moviecustomer 없는 경우? -> 생각해보니 그런경우는업따.

        cursor.execute("select id, name, age, reserve_price, score from customer, moviecustomer \
                        where customer.id = moviecustomer.customer_id and movie_id = %s \
                        order by id asc;", (movie_id,))
        result = cursor.fetchall()

        print_col_names = ('id', 'name', 'age', 'res.price', 'rating')
        print('-' * 60)
        strFormat = "%-4s%-26s%-10s%-10s%-10s"
        print(strFormat % print_col_names)
        print('-' * 60)
        for row in result:
            if row["score"] is None:
                rating = "None"
            else:
                rating = row["score"]
            print_values = (row["id"], row["name"][0:25], row["age"], row["reserve_price"], rating)
            print(strFormat % print_values)
        print('-' * 60)


    # YOUR CODE GOES HERE


# Problem 11 (5 pt.)
def print_movies_for_user():
    # YOUR CODE GOES HERE
    user_id = int(input('User ID: '))
    with connection.cursor(dictionary=True, buffered=True) as cursor:
        cursor.execute("select * from customer where id = %s;", (user_id,))
        cnt = cursor.rowcount
        if cnt == 0:
            print(f'User {user_id} does not exist')
            return

        # TODO
        cursor.execute("select id, title, director, reserve_price, score \
                       from movie, moviecustomer \
                       where movie.id = moviecustomer.movie_id and \
                       moviecustomer.customer_id = %s \
                       order by id asc;", (user_id, ))
        result = cursor.fetchall()

        print_col_names = ("id", "title", "director", "res. price", "rating")

        print('-' * 80)
        strFormat = "%-4s%-28s%-28s%-12s%-8s"
        print(strFormat % print_col_names)
        print('-' * 80)
        for row in result:
            if row["score"] is None:
                score = "None"
            else:
                score = row["score"]

            # 평점이 존재하지 않는 경우 None으로 출력
            # 예매 가격도 마찬가지라고 가정.
            print_values = (row["id"], row["title"][0:27], row["director"][0:27], row["reserve_price"], score)
            print(strFormat % print_values) # 너무 길어서 적당히 잘라서 출력
        print('-' * 80)

    # YOUR CODE GOES HERE


# Problem 12 (6 pt.)
def recommend_popularity():
    user_id = int(input('User ID: '))
    with connection.cursor(dictionary=True, buffered=True) as cursor:
        cursor.execute("select * from customer where id = %s;", (user_id,))
        cnt = cursor.rowcount
        if cnt == 0:
            print(f'User {user_id} does not exist')
            return

        # TODO
        '''cursor.execute("select id, title, reserve_price, cnt(customer_id), avg(score) \
                       from movie, moviecustomer \
                       where movie.id = moviecustomer.movie_id and \
                       (moviecustomer.customer_id <> %s or \
                       (moviecustomer.customer_id = %s and moviecustomer.reserve = %s)) \
                       avg(score) >= all ( \
                       select avg(score) from moviecustomer \
                       where (moviecustomer.customer_id <> %s or \
                       (moviecustomer.customer_id = %s and moviecustomer.reserve = %s)) \
                       group by movie.id; ) \
                       group by movie.id;")
        cnt = cursor.rowcount
        if cnt == 0:
            pass
        elif cnt > 1:'''

        # TODO: user가 예매한 id는 continue
        cursor.execute("select id from movie, moviecustomer where movie.id = moviecustomer.movie_id \
                       and moviecustomer.customer_id = %s;", (user_id, ))
        result = cursor.fetchall()
        user_reserved_ids = []
        for row in result:
            user_reserved_ids.append(row["id"])
        if DEBUG:
            print(user_reserved_ids)

        cursor.execute("select id, count(customer_id), avg(score) \
                       from movie left outer join moviecustomer on movie.id = moviecustomer.movie_id \
                       group by id;")

        # moviecustomer.customer_id <> %s는 null과의 비교를 해 주지 않으므로
        # or moviecustomer.customer_id is null 을 따로 적어야 아무도 예매하지 않은 영화도 검색됨
        # TODO: title과 (current user가 예매할 때) reserve_price는 따로 출력해야 함
        # SELECT list is not in GROUP BY clause and contains nonaggregated column

        # where문에서 id compare 시 해당 movie가 아니라 join된 row 하나만 뺴는 버그 있음

        result = cursor.fetchall()
        max_score = None
        max_score_id = None
        max_score_customers_cnt = None
        max_customers = None
        max_customers_id = None
        max_customers_score = None
        for row in result:
            current_id = row["id"]
            if current_id in user_reserved_ids:
                continue
            current_score = row["avg(score)"]
            current_customers = row["count(customer_id)"]

            # 평균 평점
            if current_score is None:
                if max_score is None:
                    if max_score_id is None or current_id < max_score_id:
                        max_score_id = current_id
                        max_score_customers_cnt = current_customers
                        # 모든 영화가 평점이 없을 경우, ID값이 작은 영화를 추천한다
                else:
                    # 어떤 영화는 평점이 있으나, 현재 영화는 평점이 없는 경우
                    pass
            else: # 현재 영화에 평점이 있는 경우
                if max_score is None:
                    max_score = current_score
                    max_score_id = current_id
                    max_score_customers_cnt = current_customers
                else:
                    if round(current_score, 10) > round(max_score, 10):
                        # floating point 오차를 고려하게 구현
                        # TODO: 필요없나?
                        max_score = current_score
                        max_score_id = current_id
                        max_score_customers_cnt = current_customers
                    elif round(current_score, 10) == round(max_score, 10) and (max_score_id is None or current_id < max_score_id):
                        # 예상된 평점이 같은 경우 ID값이 작은 영화를 추천한다
                        max_score_id = current_id
                        max_score_customers_cnt = current_customers
            # 가장 많은 고객
            # count(customer_id)는 None이 아니라 0으로 나오게 됨
            # max_customers가 None인 경우만 고려.
            if max_customers is None:
                max_customers = current_customers
                max_customers_id = current_id
                max_customers_score = current_score
            elif max_customers < current_customers:
                max_customers = current_customers
                max_customers_id = current_id
                max_customers_score = current_score
            elif max_customers == current_customers:
                if max_customers_id > current_id:
                    # 고객 수가 같을 경우 ID값이 작은 영화를 추천한다
                    max_customers = current_customers
                    max_customers_id = current_id
                    max_customers_score = current_score


            # TODO: 모든 영화의 관객이 0명인 경우? 현재는 id가 작은 영화를 추천할 듯

        # print
        # TODO: 두 영화가 같은 경우?
        # 두 영화가 같은 경우, 해당 영화(하나)만 추천하는 것으로 가정
        cursor.execute("select class from customer where id = %s;", (user_id,))
        result = cursor.fetchone()
        class_ = result["class"]

        cursor.execute("select title, price from movie where id = %s;", (max_score_id,))
        result = cursor.fetchone()
        max_score_title = result["title"]
        max_score_reserve_price = get_reserve_price(result["price"], class_)

        cursor.execute("select title, price from movie where id = %s;", (max_customers_id,))
        result = cursor.fetchone()
        max_customers_title = result["title"]
        max_customers_reserve_price = get_reserve_price(result["price"], class_)



        print_col_names = ("id", "title", "res. price", "reservation", "avg. rating")

        print('-' * 80)
        strFormat = "%-4s%-40s%-12s%-12s%-12s"
        print(strFormat % print_col_names)
        print('-' * 80)
        if max_score is None:
            max_score_print = "None"
        else:
            max_score_print = round(max_score, 2)
        print_values = (max_score_id, max_score_title[0:39], round(max_score_reserve_price, 2), max_score_customers_cnt, max_score_print)
        print(strFormat % print_values)
        if max_customers_id != max_score_id:
            if max_customers_score is None:
                max_customers_score_print = "None"
            else:
                max_customers_score_print = round(max_customers_score, 2)
            print_values = (max_customers_id, max_customers_title[0:39], round(max_customers_reserve_price, 2), max_customers, max_customers_score_print)
            print(strFormat % print_values)
        print('-' * 80)

# Problem 13 (10 pt.)
def recommend_item_based():
    # YOUR CODE GOES HERE
    user_id = int(input('User ID: '))
    rec_count = int(input('Recommend Count: '))
    with connection.cursor(dictionary=True, buffered=True) as cursor:
        cursor.execute("select * from customer where id = %s;", (user_id,))
        cnt = cursor.rowcount
        if cnt == 0:
            print(f'User {user_id} does not exist')
            return
        cursor.execute("select * from moviecustomer \
                        where customer_id = %s and \
                        score is not null;", (user_id,))
        cnt = cursor.rowcount
        if cnt == 0:
            print('Rating does not exist')
            return

        # 어떤 영화에도 평점을 매기지 않은 고객은 matrix를 생성 시 제외
        cursor.execute("select customer_id, count(score) from moviecustomer \
                        group by customer_id;") # score가 null이면 자동 제외
        result = cursor.fetchall()
        matrix_users = [] # matrix의 user order
        for row in result:
            if row["count(score)"] > 0:
                matrix_users.append(row["customer_id"])
        user_cnt = len(matrix_users) # TODO: 이거 0이면 어캄

        cursor.execute("select id from movie order by id asc;")
        result = cursor.fetchall()
        movie_ids = []
        for row in result:
            movie_ids.append(row["id"])
        movie_cnt = len(movie_ids)

        matrix_item = [[0 for _ in range(movie_cnt)] for _ in range(user_cnt)] # users 행 movies 열

        cursor.execute("select movie_id, customer_id, score from moviecustomer where score is not null order by movie_id asc;")
        result = cursor.fetchall()
        for row in result:
            user_idx = matrix_users.index(row["customer_id"])
            movie_idx = movie_ids.index(row["movie_id"])
            matrix_item[user_idx][movie_idx] = row["score"] # user-item matrix 초기화

        # ith row: matrix_users[i] 에 해당하는 user
        # jth column: movie_ids[j]에 해당하는 movie

        for current_movie in range(movie_cnt): # 평균으로 임시 평점 부여
            cnt = 0
            avg = 0
            for i in range(len(matrix_item)):
                if matrix_item[i][current_movie] != 0:
                    cnt += 1
                    avg += matrix_item[i][current_movie]
            if cnt == 0:
                continue
            avg /= cnt
            for i in range(len(matrix_item)):
                if matrix_item[i][current_movie] == 0:
                    matrix_item[i][current_movie] = avg

        if DEBUG:
            for i in range(user_cnt):
                for j in range(movie_cnt):
                    print(matrix_item[i][j], end=' ')
                print()

        matrix_similarity = [[0 for _ in range(movie_cnt)] for _ in range(movie_cnt)]
        # TODO: movie * movie 맞나
        matrix_item_avg = 0
        for i in range(len(matrix_item)):
            for j in range(len(matrix_item[0])):
                matrix_item_avg += matrix_item[i][j]
        matrix_item_avg /= (user_cnt * movie_cnt)

        for i in range(movie_cnt):
            for j in range(movie_cnt):
                if i==j:
                    matrix_similarity[i][j] = 1
                elif i<j:
                    # TODO
                    sum_ab = 0
                    sum_sqrt_a = 0
                    sum_sqrt_b = 0
                    for usr in range(user_cnt):
                        a_i = matrix_item[usr][i]
                        b_i = matrix_item[usr][j]
                        sum_ab += (a_i-matrix_item_avg) * (b_i-matrix_item_avg)
                        sum_sqrt_a += (a_i-matrix_item_avg) * (a_i-matrix_item_avg)
                        sum_sqrt_b += (b_i-matrix_item_avg) * (b_i-matrix_item_avg)
                    sum_sqrt_a = pow(sum_sqrt_a, 0.5)
                    sum_sqrt_b = pow(sum_sqrt_b, 0.5)

                    matrix_similarity[i][j] = round(sum_ab / (sum_sqrt_a * sum_sqrt_b), 5)

                else:
                    continue # symmetric

        for i in range(movie_cnt):
            for j in range(movie_cnt):
                if i<=j:
                    continue
                else:
                    matrix_similarity[i][j] = matrix_similarity[j][i]

        if DEBUG:
            for i in range(movie_cnt):
                for j in range(movie_cnt):
                    print(matrix_similarity[i][j], end=' ')
                print()

        cursor.execute("select movie_id from moviecustomer \
                        where customer_id = %s and \
                        score is not null;", (user_id, ))
        result = cursor.fetchall()
        scored_movies_idx = []
        for row in result:
            scored_movies_idx.append(movie_ids.index(row["movie_id"]))


        cursor.execute("select movie_id from moviecustomer where customer_id = %s;", (user_id,))
        result = cursor.fetchall()
        watched_movies_idx = []
        for row in result:
            watched_movies_idx.append(movie_ids.index(row["movie_id"]))

        i = matrix_users.index(user_id)
        pred_scores = matrix_item[i].copy() # deep copy


        # weighted sum
        for j in range(movie_cnt): # user i, item j에 대하여 item j의 평점을 weighted sum으로 계산
            if j in scored_movies_idx:
                continue
            weighted_sum = 0
            weights = 0
            for k in range(movie_cnt): # item j, item k의 similarity
                if j == k:
                    continue
                current_weight = matrix_similarity[j][k]
                weights += current_weight
                weighted_sum += current_weight * matrix_item[i][k] # pred_scores는 값이 계속 변하기 때문에...
                if DEBUG:
                    print(f"{current_weight}    {matrix_item[i][k]}")
            weighted_sum /= weights
            pred_scores[j] = weighted_sum
            # TODO: 임시 평점도 써야 함

        if DEBUG:
            print(pred_scores)

        predicted_scores_dict = {}
        for j in range(movie_cnt):
            if j not in watched_movies_idx:
                predicted_scores_dict[j] = pred_scores[j] # {index : predicted score}

        recommend_dict = sorted(predicted_scores_dict.items(), key=lambda item: item[1], reverse=True)
        if DEBUG:
            print(recommend_dict)
        # predicted score의 내림차순으로 sort

        print_col_names = ("id", "title", "res. price", "avg. rating", "expected rating")

        print('-' * 80)
        strFormat = "%-4s%-37s%-12s%-12s%-15s"
        print(strFormat % print_col_names)
        print('-' * 80)

        cursor.execute("select class from customer where id = %s;", (user_id,))
        result = cursor.fetchone()
        class_ = result["class"]

        cnt = 0
        for index, predicted_score in recommend_dict:
            if index in watched_movies_idx:
                continue
            cursor.execute("select id, title, price, avg(score) from movie, moviecustomer \
                            where movie.id = moviecustomer.movie_id and \
                            id = %s \
                            group by id;", (movie_ids[index],))
            result = cursor.fetchone()
            id = result["id"]
            title = result["title"]
            price = result["price"]
            avg_score = result["avg(score)"]

            reserve_price = get_reserve_price(price, class_)

            print_values = (id, title, reserve_price, avg_score, round(predicted_score, 5))

            print(strFormat % print_values)
            cnt += 1
            if cnt >= rec_count:
                break
        print('-' * 80)

# Total of 70 pt.
def main():
    # initialize database

    while True:
        print('============================================================')
        print('1. initialize database')
        print('2. print all movies')
        print('3. print all users')
        print('4. insert a new movie')
        print('5. remove a movie')
        print('6. insert a new user')
        print('7. remove an user')
        print('8. book a movie')
        print('9. rate a movie')
        print('10. print all users who booked for a movie')
        print('11. print all movies rated by an user')
        # spec에 rated가 아니라 booked로 명시되어 있으므로 booked로 구현
        print('12. recommend a movie for a user using popularity-based method')
        print('13. recommend a movie for a user using item-based collaborative filtering')
        print('14. exit')
        print('15. reset database')
        print('============================================================')
        menu = int(input('Select your action: '))

        if menu == 1:
            initialize_database()
        elif menu == 2:
            print_movies()
        elif menu == 3:
            print_users()
        elif menu == 4:
            insert_movie()
        elif menu == 5:
            remove_movie()
        elif menu == 6:
            insert_user()
        elif menu == 7:
            remove_user()
        elif menu == 8:
            book_movie()
        elif menu == 9:
            rate_movie()
        elif menu == 10:
            print_users_for_movie()
        elif menu == 11:
            print_movies_for_user()
        elif menu == 12:
            recommend_popularity()
        elif menu == 13:
            recommend_item_based()
        elif menu == 14:
            print('Bye!')
            connection.close()
            break
        elif menu == 15:
            reset()
        else:
            print('Invalid action')


if __name__ == "__main__":
    main()
