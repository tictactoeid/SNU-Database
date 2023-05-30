#import pymysql
import numpy as np
import pandas as pd
import mysql.connector

connection = mysql.connector.connect(
    #auth_plugin='mysql_native_password',
    host='astronaut.snu.ac.kr',
    port=7000,
    user='DB2020_15127',
    password='DB2020_15127',
    db='DB2020_15127',
    charset='utf8'
)

DEBUG = True # TODO: make it False
# TODO: connection.commit()

# Problem 1 (5 pt.)
def initialize_database():
    # YOUR CODE GOES HERE
    # TODO: ID
    with connection.cursor(dictionary=True) as cursor:
        cursor.execute("drop table director;")
        cursor.execute("drop table customer;")
        cursor.execute("drop table movie;")
        cursor.execute("drop table moviecustomer;")

        cursor.execute("create table director ( name char(100), primary key (name) );")
        cursor.execute("create table customer ( id int auto_increment, name char(100), age int, class char(100), primary key (id) );")
        cursor.execute("create table movie ( id int auto_increment, title char(100), director_name char(100), price int, primary key (id), foreign key (director_name) references director (name) );")
        cursor.execute("create table moviecustomer ( movie_id int, customer_id int, reserve boolean, score int, reserve_price int, foreign key (customer_id) references customer (id), foreign key (movie_id) references movie (id) );")
        connection.commit()

        data = pd.read_csv("./data.csv")
        for row in data.iterrows():
            title = row["title"]
            director = row["director"]
            price = row["price"]
            name = row["name"]
            age = row["age"]
            class_ = row["class"]

            #sql = "insert into director values (%s);"
            #val = (director,)
            #cursor.execute(sql, val)
            cursor.execute("insert into director values (%s);", (director,))
            cursor.execute("insert into movie values (%s, %s, %s);", (title, director, price))
            movie_id = connection.insert_id()
            cursor.execute("insert into customer values (%s, %s, %s);", (name, age, class_))
            customer_id = connection.insert_id()
            cursor.execute("insert into moviecustomer values (%s, %s, %s, %s, %s);", (movie_id, customer_id, False, None, None));
            connection.commit()

    print('Database successfully initialized')
    # YOUR CODE GOES HERE


# Problem 15 (5 pt.)
def reset():
    # YOUR CODE GOES HERE
    initialize_database()
    # TODO
    # YOUR CODE GOES HERE


# Problem 2 (4 pt.)
def print_movies():
    # YOUR CODE GOES HERE
    with connection.cursor(dictionary=True) as cursor:
        cursor.execute("select id, title, director_name, avg(reserved_price), \
                       count(customer_id), avg(score) from movie, moviecustomer \
                       where movie.id = moviecustomer.movie_id\
                       group by id, order by id asc;")
        result = cursor.fetchall()
        print(result)
    # TODO: 영화에 대한 평점이 존재하지 않는다면 ‘None’ 으로 출력한다.
    # YOUR CODE GOES HERE

# Problem 3 (3 pt.)
def print_users():
    # YOUR CODE GOES HERE
    with connection.cursor(dictionary=True) as cursor:
        cursor.execute("select id, name, age, class from customer order by id asc;")
        result = cursor.fetchall()
        print(result)
    # YOUR CODE GOES HERE


# Problem 4 (4 pt.)
def insert_movie():
    # YOUR CODE GOES HERE
    title = input('Movie title: ')
    director = input('Movie director: ')
    price = input('Movie price: ')
    try:
        price = int(price)
        if price < 0 or price > 100000:
            print('Movie price should be from 0 to 100000')
            return
    except ValueError:
        print('Movie price should be from 0 to 100000')
        return

    with connection.cursor(dictionary=True) as cursor:
        cursor.execute("select * from movie where title = %s;", (title,))
        cnt = cursor.rowcount
        if cnt != 0:
            print(f'Movie {title} already exists')
            return

        cursor.execute("insert into director values (%s);", (director,))
        cursor.execute("insert into movie values (%s, %s, %s);", (title, director, price))
        print('One movie successfully inserted')


# Problem 6 (4 pt.)
def remove_movie():
    # YOUR CODE GOES HERE
    movie_id = input('Movie ID: ')
    with connection.cursor(dictionary=True) as cursor:
        cursor.execute("select * from movie where id = %s;", (movie_id,))
        cnt = cursor.rowcount
        if cnt == 0:
            print(f'Movie {movie_id} does not exist')
            return
        cursor.execute("delete from moviecustomer where movie_id = %s;", (movie_id,))
        cursor.execute("delete from movie where id = %s;", (movie_id,))
        print('One movie successfully removed')

# Problem 5 (4 pt.)
def insert_user():
    # YOUR CODE GOES HERE
    name = input('User name: ')
    age = input('User age: ')
    class_ = input('User class: ').lower() # name, title 등은 대소문자를 구분하나, class는 구분하지 않는다고 가정
    try:
        age = int(age)
        if age < 12 or age > 110:
            print('User age should be from 12 to 110')
            return
    except ValueError:
        print('User age should be from 12 to 110')
        return
    if class_ not in ["basic", "premium", "vip"]:
        print('User class should be basic, premium or vip')
        return

    with connection.cursor(dictionary=True) as cursor:
        cursor.execute("select * from customer where name = %s and age =  %s;", (name, age))
        cnt = cursor.rowcount
        if cnt != 0:
            print(f'The user ({name}, {age}) already exists')
            return

        cursor.execute("insert into customer values (%s, %s, %s);", (name, age, class_))
        print('One user successfully inserted')

# Problem 7 (4 pt.)
def remove_user():
    # YOUR CODE GOES HERE
    user_id = input('User ID: ')
    with connection.cursor(dictionary=True) as cursor:
        cursor.execute("select * from customer where id = %s;", (user_id,))
        cnt = cursor.rowcount
        if cnt == 0:
            print(f'User {user_id} does not exist')
            return
        cursor.execute("delete from moviecustomer where user_id = %s;", (user_id,))
        cursor.execute("delete from customer where id = %s;", (user_id,))
        print('One user successfully removed')
    # TODO: delete 평점

# Problem 8 (5 pt.)
def book_movie():
    # YOUR CODE GOES HERE
    movie_id = input('Movie ID: ')
    user_id = input('User ID: ')

    # error message
    with connection.cursor(dictionary=True) as cursor:
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

        if class_.lower() == "basic":
            reserve_price = price
        elif class_.lower() == "premium":
            reserve_price = int(price*0.75)
        elif class_.lower() == "vip":
            reserve_price = int(price*0.5)

        cursor.execute("select * from moviecustomer where movie_id = %s and user_id = %s;", (movie_id, user_id))
        cnt = cursor.rowcount
        if cnt != 0:
            # row exists
            result = cursor.fetchone()
            if result["reserve"]: # True
                print(f'User {user_id} already booked movie {movie_id}')
                return
            # not reserved yet
            cursor.execute("update moviecustomer set reserve = %s, reserve_price = %s where movie_id = %s and user_id = %s;", (True, reserve_price, movie_id, user_id))

        else:
            # row not exists
            cursor.execute("insert into moviecustomer values (%s, %s, %s, %s, %s);", (movie_id, user_id, True, 0, reserve_price))

    print('Movie successfully booked')
    # YOUR CODE GOES HERE

# Problem 9 (5 pt.)
def rate_movie():
    # YOUR CODE GOES HERE
    movie_id = input('Movie ID: ')
    user_id = input('User ID: ')
    rating = input('Ratings (1~5): ')

    # error message
    print(f'Movie {movie_id} does not exist')
    print(f'User {user_id} does not exist')
    print(f'Wrong value for a rating')
    print(f'User {user_id} has not booked movie {movie_id} yet')
    print(f'User {user_id} has already rated movie {movie_id}')

    # success message
    print('Movie successfully rated')
    # YOUR CODE GOES HERE
    pass


# Problem 10 (5 pt.)
def print_users_for_movie():
    # YOUR CODE GOES HERE
    user_id = input('User ID: ')

    # error message
    print(f'User {user_id} does not exist')
    # YOUR CODE GOES HERE
    pass


# Problem 11 (5 pt.)
def print_movies_for_user():
    # YOUR CODE GOES HERE
    user_id = input('User ID: ')

    # error message
    print(f'User {user_id} does not exist')
    # YOUR CODE GOES HERE
    pass


# Problem 12 (6 pt.)
def recommend_popularity():
    # YOUR CODE GOES HERE
    user_id = input('User ID: ')

    # error message
    print(f'User {user_id} does not exist')
    # YOUR CODE GOES HERE
    pass


# Problem 13 (10 pt.)
def recommend_item_based():
    # YOUR CODE GOES HERE
    user_id = input('User ID: ')

    # error message
    print(f'User {user_id} does not exist')
    print('Rating does not exist')
    # YOUR CODE GOES HERE
    pass


# Total of 70 pt.
def main():
    # initialize database
    reset()

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
