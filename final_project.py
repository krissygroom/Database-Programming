import sqlite3
import re

# connect to sqlite database and name it final
db = sqlite3.connect("final.sqlite")


# drop tables if they exist
db.execute("DROP TABLE IF EXISTS ratings")
db.execute("DROP TABLE IF EXISTS users")
db.execute("DROP TABLE IF EXISTS movie_genres")
db.execute("DROP TABLE IF EXISTS movies")
db.execute("DROP TABLE IF EXISTS ages")
db.execute("DROP TABLE IF EXISTS occupations")
db.execute("DROP TABLE IF EXISTS genres")


# creating tables

# ages table
db.execute(""" CREATE TABLE IF NOT EXISTS ages (
                    ageid integer PRIMARY KEY,
                    agerange text NOT NULL) 
                    """)

db.execute("""INSERT INTO ages(
                ageid,
                agerange)
            VALUES 
                (1,  'Under 18'),
                (18, '18-24'),
                (25, '25-34'),
                (35, '35-44'),
                (45, '45-49'),
                (50, '50-55'),
                (55, '55+')""")

# occupations table
db.execute(""" CREATE TABLE IF NOT EXISTS occupations (
                    occupationid integer PRIMARY KEY,
                    occupation text NOT NULL) 
                    """)

db.execute("""INSERT INTO occupations(
                occupationid,
                occupation)
            VALUES 
                (0,  '"other" or not specified'),
                (1,  'academic/educator'),
                (2,  'artist'),
                (3,  'clerical/admin'),
                (4,  'college/grad student'),
                (5,  'customer service'),
                (6,  'doctor/health care'),
                (7,  'executive/managerial'),
                (8,  'farmer'),
                (9,  'homemaker'),
                (10,  'K-12 student'),
                (11,  'lawyer'),
                (12,  'programmer'),
                (13,  'retired'),
                (14,  'sales/marketing'),
                (15,  'scientist'),
                (16,  'self-employed'),
                (17,  'technician/engineer'),
                (18,  'tradesman/craftsman'),
                (19,  'unemployed'),
                (20,  'writer')""")

# genres table
db.execute(""" CREATE TABLE IF NOT EXISTS genres (
                    genre text PRIMARY KEY) 
                    """)

db.execute("""INSERT INTO genres(
                genre)
            VALUES 
                ('Action'),
                ('Adventure'),
                ('Animation'),
                ('Children''s'),
                ('Comedy'),
                ('Crime'),
                ('Documentary'),
                ('Drama'),
                ('Fantasy'),
                ('Film-Noir'),
                ('Horror'),
                ('Musical'),
                ('Mystery'),
                ('Romance'),
                ('Sci-Fi'),
                ('Thriller'),
                ('War'),
                ('Western')""")


# movies table
db.execute(""" CREATE TABLE IF NOT EXISTS movies (
                    movieid integer PRIMARY KEY,
                    title text NOT NULL,
                    year integer) 
                    """)

db.execute(""" CREATE TABLE IF NOT EXISTS movie_genres (
                    movieid integer,
                    genre text,
                    PRIMARY KEY (movieid, genre),
                    FOREIGN KEY (movieid) REFERENCES movies (movieid),
                    FOREIGN KEY (genre) REFERENCES genres (genre))
                    """)

# users table
db.execute(""" CREATE TABLE IF NOT EXISTS users (
                    userid integer PRIMARY KEY,
                    gender text,
                    agecode integer,
                    occupationid integer,
                    zipcode text,
                    FOREIGN KEY (occupationid) REFERENCES occupations (occupationid),
                    FOREIGN KEY (agecode) REFERENCES ages (ageid)) 
                    """)


# ratings table
db.execute(""" CREATE TABLE IF NOT EXISTS ratings (
                    userid integer,
                    movieid integer,
                    rating integer,
                    timestamp integer,
                    PRIMARY KEY (userid, movieid),
                    FOREIGN KEY (userid) REFERENCES users (userid),
                    FOREIGN KEY (movieid) REFERENCES movies (movieid)) 
                    """)


# get movie from movies.dat
# add movie id and genre into movie_genres table
# find year and add to its own column
# populate movies table

# counting lines from movies
m_lines = 0

try:
    with open("/Users/kristengroom/Desktop/final/movies.txt", "r") as myfile:
        for line in myfile:
            line = line.strip().split('::')
            gens = line[2].strip().split('|')

            # populating movie_genres with movieid and genre from the genre list, split
            for g in gens:
                db.execute("""INSERT INTO movie_genres(
                                movieid, 
                                genre)
                            VALUES (?, ?)""", [line[0], g])

            # getting the year out of title and putting it into its own column
            year_index = [(y.start(0)) for y in re.finditer(r'\((\d{4})\)', line[1])]
            year_to_extract = year_index[0] - 1
            title = line[1]
            title_no_year = title[:year_to_extract]

            # get the substring of year
            year = re.findall(r'\((\d{4})\)', line[1])
            m_lines += 1

            # populating movies table
            db.execute("""INSERT INTO movies(
                                movieid, 
                                title,
                                year)
                            VALUES (?, ?, ?)""", [line[0], title_no_year, int(year[0])])
except IOError:
    print("An error occurred when trying to read in movies file.")

# check if all files were read in
print("{} lines were read from movies.txt".format(m_lines))


# Populating users table:

# counting lines read in
u_lines = 0

# # read in users file and change age to agecode then populate users table
try:
    with open("/Users/kristengroom/Desktop/final/users.txt", "r") as myfile:
        for line in myfile:
            line = line.split('::')

            # finding age to use for agecode
            age = int(line[2])
            age_code = 1
            if 18 <= age < 25:
                age_code = 18
            elif 25 <= age < 35:
                age_code = 25
            elif 35 <= age < 45:
                age_code = 35
            elif 45 <= age < 50:
                age_code = 45
            elif 50 <= age < 55:
                age_code = 50
            elif age >= 55:
                age_code = 55

            # now populate users table:
            db.execute("""INSERT INTO users(
                                    userid, 
                                    gender,
                                    agecode,
                                    occupationid,
                                    zipcode)
                                VALUES (?, ?, ?, ?, ?)""", [line[0], line[1], age_code, line[3], line[4].strip()])
            u_lines += 1

except IOError:
    print("An error occurred when trying to read in users file")

print("{} lines were read from users.txt".format(u_lines))


# Populating Ratings table:
# counting lines read in
r_lines = 0

# read in ratings file
try:
    with open("/Users/kristengroom/Desktop/final/ratings.txt", "r") as myfile:
        for line in myfile:
            line = line.split('::')

            # populate ratings table:
            db.execute("""INSERT INTO ratings(
                                    userid, 
                                    movieid,
                                    rating,
                                    timestamp)
                                VALUES (?, ?, ?, ?)""", [line[0], line[1], line[2], line[3]])
            r_lines += 1

except IOError:
    print("An error occurred when trying to read in ratings file")

print("{} lines were read from ratings.txt".format(r_lines))


# interesting query
# I wanted to find the most highly rated genre in each age group
# I got stuck on finding the max of the age-range so I decided to
# stick with what I had that was working...
# This is what are the average ratings for each genre in each age category:

sql_query1 = """select a.agerange, mg.genre, round(avg(rating),2) as average
                from ages a, users u, movie_genres mg, ratings r
                where u.agecode = a.ageid
                and u.userid = r.userid 
                and mg.movieid = r.movieid
                group by agerange, genre
                order by a.agerange;"""

print("\nQuery 1: Find average ratings for each genre in each age category:\n")
for row in db.execute(sql_query1):
    print(row)


print('\n' + '-'*30+ '\n')
# second query: find the number of movie ratings for each
# occupation and then order from highest to lowest to
# see which occupations do the most movie ratings:

sql_query2 = """
        select occupation, count(rating) movie_ratings
        from occupations o, ratings r, users u
        where o.occupationid = u.occupationid
        and r.userid = u.userid
        group by occupation
        order by movie_ratings DESC;
        """

print("Query 2: Find number of ratings per occupation in descending order:")
print()
for row in db.execute(sql_query2):
    print(row)

# commit all changes
db.commit()
# close the database
db.close()
