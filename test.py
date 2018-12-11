import requests
import json
from bs4 import BeautifulSoup
import secret
import sqlite3


API_KEY = secret.api_key
PLOTLY_API_KEY = secret.PLOTLY_API_KEY
CACHE_FNAME1 = 'douban_cache.json'
CACHE_FNAME2 = 'imdb_cache.json'
DBNAME = 'movie.db'

def params_unique_combination(baseurl, params):
    if type(params) == dict:
        alphabetized_keys = sorted(params.keys())
        res = []
        for k in alphabetized_keys:
            res.append("{}-{}".format(k, params[k]))
        return baseurl + "_" + "_".join(res)
    else:
        return baseurl + params


def create_cache(cache_name):
    try:
        cache_file = open(cache_name, 'r')
        cache_contents = cache_file.read()
        CACHE_DICTION = json.loads(cache_contents)
        cache_file.close()
    except:
        CACHE_DICTION = {}
    return CACHE_DICTION


CACHE_DICTION1 = create_cache(CACHE_FNAME1)
CACHE_DICTION2 = create_cache(CACHE_FNAME2)


def requests_catch(baseurl, para, cache_name):
    unique_ident = params_unique_combination(baseurl, para)
    CACHE_DICTION = create_cache(cache_name)
    if unique_ident in CACHE_DICTION:
        # print("Getting cached data...")
        return CACHE_DICTION[unique_ident]
    else:
        print("Making a request for new data...")
        if type(para) == dict:
            resp = requests.get(baseurl, params = para)
        else:
            resp = requests.get(baseurl+para)
        if type(para) == dict:
            CACHE_DICTION[unique_ident] = json.loads(resp.text)
        else:
            CACHE_DICTION[unique_ident] = resp.text
        dumped_json_cache = json.dumps(CACHE_DICTION)
        fw = open(cache_name,"w")
        fw.write(dumped_json_cache)
        fw.close()
        return CACHE_DICTION[unique_ident]


def catch_retrived(baseurl, para, cache_name):
    unique_ident = params_unique_combination(baseurl, para)
    CACHE_DICTION = create_cache(cache_name)
    return CACHE_DICTION[unique_ident]

class Movie():
    def __init__(self, imdb_title, item_title, imdb_year, imdb_runtime, imdb_country, imdb_languages, imdb_quote, item_quote, imdb_poster):
        self.EngTitle = imdb_title
        self.ChiTitle = item_title
        self.Year = imdb_year
        self.Runtime = imdb_runtime
        self.Country = imdb_country
        self.Language = imdb_languages
        self.EngQuote = imdb_quote
        self.ChiQuote = item_quote
        self.Poster = imdb_poster

    def __str__(self):
        return self.EngTitle



def init_db():
    try:
        conn = sqlite3.connect(DBNAME)
    except:
        print("fail to connect to database")
        return
    cur = conn.cursor()
    # Drop tables
    statement = '''
        DROP TABLE IF EXISTS 'Movies';
    '''
    cur.execute(statement)
    statement = '''
        DROP TABLE IF EXISTS 'Views';
    '''
    cur.execute(statement)

    statement = '''
        DROP TABLE IF EXISTS 'Sources';
    '''
    cur.execute(statement)

    statement = '''
        DROP TABLE IF EXISTS 'Actors';
    '''
    cur.execute(statement)

    statement = '''
        DROP TABLE IF EXISTS 'Directors';
    '''
    cur.execute(statement)

    statement = '''
        DROP TABLE IF EXISTS 'Genres';
    '''
    cur.execute(statement)

    statement = '''
        DROP TABLE IF EXISTS 'Tags';
    '''
    cur.execute(statement)

    statement = '''
        DROP TABLE IF EXISTS 'ActorMovie';
    '''
    cur.execute(statement)

    statement = '''
        DROP TABLE IF EXISTS 'DirectorMovie';
    '''
    cur.execute(statement)

    statement = '''
        DROP TABLE IF EXISTS 'GenreMovie';
    '''
    cur.execute(statement)

    statement = '''
        DROP TABLE IF EXISTS 'TagMovie';
    '''
    cur.execute(statement)
    conn.commit()



    statement = '''
        CREATE TABLE 'Movies' (
            'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'EngTitle' TEXT NOT NULL,
            'ChiTitle' TEXT NOT NULL,
            'Year' INTEGER NOT NULL,
            'Runtime' INTEGER NOT NULL,
            'Country' TEXT NOT NULL,
            'Language' TEXT NOT NULL,
            'EngQuote' TEXT NOT NULL,
            'ChiQuote' TEXT NOT NULL,
            'Poster' TEXT NOT NULL
        );
    '''
    cur.execute(statement)

    statement = '''
        CREATE TABLE 'Views' (
                'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
                'Rank' INTEGER,
                'Rating' REAL NOT NULL,
                'Votes' INTEGER NOT NULL,
                'SourceId' INTEGER NOT NULL,
                'MovieId' INTEGER NOT NULL
        );
    '''
    cur.execute(statement)

    statement = '''
        CREATE TABLE 'Sources' (
                'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
                'Name' TEXT
        );
    '''
    cur.execute(statement)

    statement = '''
        CREATE TABLE 'Actors' (
                'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
                'Name' TEXT,
                'SourceId' INTEGER NOT NULL,
                UNIQUE('Name', 'SourceId')
        );
    '''
    cur.execute(statement)

    statement = '''
        CREATE TABLE 'Directors' (
                'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
                'Name' TEXT,
                'SourceId' INTEGER NOT NULL,
                UNIQUE('Name', 'SourceId')
        );
    '''
    cur.execute(statement)

    statement = '''
        CREATE TABLE 'Genres' (
                'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
                'Genre' TEXT,
                'SourceId' INTEGER NOT NULL,
                UNIQUE('Genre', 'SourceId')
        );
    '''
    cur.execute(statement)

    statement = '''
        CREATE TABLE 'Tags' (
                'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
                'Tag' TEXT,
                'SourceId' INTEGER NOT NULL,
                UNIQUE('Tag', 'SourceId')
        );
    '''
    cur.execute(statement)

    statement = '''
        CREATE TABLE 'ActorMovie' (
                'ActorId' INTEGER NOT NULL,
                'MovieId' INTEGER NOT NULL,
                'SourceId' INTEGER NOT NULL
        );
    '''
    cur.execute(statement)

    statement = '''
        CREATE TABLE 'DirectorMovie' (
                'DirectorId' INTEGER NOT NULL,
                'MovieId' INTEGER NOT NULL,
                'SourceId' INTEGER NOT NULL
        );
    '''
    cur.execute(statement)

    statement = '''
        CREATE TABLE 'GenreMovie' (
                'GenreId' INTEGER NOT NULL,
                'MovieId' INTEGER NOT NULL,
                'SourceId' INTEGER NOT NULL
        );
    '''
    cur.execute(statement)

    statement = '''
        CREATE TABLE 'TagMovie' (
                'TagId' INTEGER NOT NULL,
                'MovieId' INTEGER NOT NULL,
                'SourceId' INTEGER NOT NULL
        );
    '''
    cur.execute(statement)
    conn.commit()
    conn.close()
    print("init DB successfully!")

def insert_source():
    try:
        conn = sqlite3.connect(DBNAME)
    except:
        print("fail to connect to database")
        return
    cur = conn.cursor()
    insertion = (None, "douban")
    statement = '''
                INSERT INTO Sources
                VALUES (?, ?)
            '''
    cur.execute(statement, insertion)
    insertion = (None, "imdb")
    statement = '''
                INSERT INTO Sources
                VALUES (?, ?)
            '''
    cur.execute(statement, insertion)
    conn.commit()
    conn.close()


def insert_data1(movie, view1, view2, actor1, actor2, director1, director2, genre1, genre2, tag1, tag2):
    try:
        conn = sqlite3.connect(DBNAME)
    except:
        print("fail to connect to database")
        return
    cur = conn.cursor()
    insertion = (None, movie.EngTitle, movie.ChiTitle, movie.Year, movie.Runtime, movie.Country, movie.Language, movie.EngQuote, movie.ChiQuote, movie.Poster)
    statement = '''
                INSERT INTO Movies
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
    cur.execute(statement, insertion)

    insertion = (None, view1[0], view1[1], view1[2], view1[3], view1[4])
    statement = '''
                INSERT INTO Views
                VALUES (?, ?, ?, ?, ?, ?)
            '''
    cur.execute(statement, insertion)

    insertion = (None, view2[0], view2[1], view2[2], view2[3], view2[4])
    statement = '''
                INSERT INTO Views
                VALUES (?, ?, ?, ?, ?, ?)
            '''
    cur.execute(statement, insertion)

    for actor in actor1:
        insertion = (None, actor, 1)
        statement = '''
                    INSERT OR IGNORE INTO Actors
                    VALUES (?, ?, ?)
                '''
        cur.execute(statement, insertion)

    for actor in actor2:
        insertion = (None, actor, 2)
        statement = '''
                    INSERT OR IGNORE INTO Actors
                    VALUES (?, ?, ?)
                '''
        cur.execute(statement, insertion)

    for director in director1:
        insertion = (None, director, 1)
        statement = '''
                    INSERT OR IGNORE INTO Directors
                    VALUES (?, ?, ?)
                '''
        cur.execute(statement, insertion)

    for director in director2:
        insertion = (None, director, 2)
        statement = '''
                    INSERT OR IGNORE INTO Directors
                    VALUES (?, ?, ?)
                '''
        cur.execute(statement, insertion)

    for genre in genre1:
        insertion = (None, genre, 1)
        statement = '''
                    INSERT OR IGNORE INTO Genres
                    VALUES (?, ?, ?)
                '''
        cur.execute(statement, insertion)

    for genre in genre2:
        insertion = (None, genre, 2)
        statement = '''
                    INSERT OR IGNORE INTO Genres
                    VALUES (?, ?, ?)
                '''
        cur.execute(statement, insertion)

    for tag in tag1:
        insertion = (None, tag, 1)
        statement = '''
                    INSERT OR IGNORE INTO Tags
                    VALUES (?, ?, ?)
                '''
        cur.execute(statement, insertion)

    for tag in tag2:
        insertion = (None, tag, 2)
        statement = '''
                    INSERT OR IGNORE INTO Tags
                    VALUES (?, ?, ?)
                '''
        cur.execute(statement, insertion)

    conn.commit()
    conn.close()

def insert_data2(movieId, actor1, actor2, director1, director2, genre1, genre2, tag1, tag2):
    try:
        conn = sqlite3.connect(DBNAME)
    except:
        print("fail to connect to database")
        return
    cur = conn.cursor()
    for actor in actor1:
        statement = '''
                    SELECT Id FROM Actors WHERE [Name] = \"{}\"
        '''
        try:
            actorId = cur.execute(statement.format(actor)).fetchone()[0]
        except:
            print(actor)
            continue
        insertion = (actorId, movieId, 1)
        statement = '''
                        INSERT INTO ActorMovie
                        VALUES (?, ?, ?)
                    '''
        cur.execute(statement, insertion)

    for actor in actor2:
        statement = '''
                    SELECT Id FROM Actors WHERE [Name] = \"{}\"
        '''
        try:
            actorId = cur.execute(statement.format(actor)).fetchone()[0]
        except:
            print(actor)
            continue
        insertion = (actorId, movieId, 2)
        statement = '''
                        INSERT INTO ActorMovie
                        VALUES (?, ?, ?)
                    '''
        cur.execute(statement, insertion)

    for director in director1:
        statement = '''
                    SELECT Id FROM Directors WHERE [Name] = \"{}\"
        '''
        try:
            directorId = cur.execute(statement.format(director)).fetchone()[0]
        except:
            print(director)
            continue
        insertion = (directorId, movieId, 1)
        statement = '''
                        INSERT INTO DirectorMovie
                        VALUES (?, ?, ?)
                    '''
        cur.execute(statement, insertion)

    for director in director2:
        statement = '''
                    SELECT Id FROM Directors WHERE [Name] = \"{}\"
        '''
        try:
            directorId = cur.execute(statement.format(director)).fetchone()[0]
        except:
            print(director)
            continue
        insertion = (directorId, movieId, 2)
        statement = '''
                        INSERT INTO DirectorMovie
                        VALUES (?, ?, ?)
                    '''
        cur.execute(statement, insertion)

    for genre in genre1:
        statement = '''
                    SELECT Id FROM Genres WHERE Genre = \"{}\"
        '''
        try:
            genreId = cur.execute(statement.format(genre)).fetchone()[0]
        except:
            continue
        insertion = (genreId, movieId, 1)
        statement = '''
                        INSERT INTO GenreMovie
                        VALUES (?, ?, ?)
                    '''
        cur.execute(statement, insertion)

    for genre in genre2:
        statement = '''
                    SELECT Id FROM Genres WHERE Genre = \"{}\"
        '''
        try:
            genreId = cur.execute(statement.format(genre)).fetchone()[0]
        except:
            continue
        insertion = (genreId, movieId, 2)
        statement = '''
                        INSERT INTO GenreMovie
                        VALUES (?, ?, ?)
                    '''
        cur.execute(statement, insertion)

    for tag in tag1:
        statement = '''
                    SELECT Id FROM Tags WHERE Tag = \"{}\"
        '''
        try:
            tagId = cur.execute(statement.format(tag)).fetchone()[0]
        except:
            continue
        insertion = (tagId, movieId, 1)
        statement = '''
                        INSERT INTO TagMovie
                        VALUES (?, ?, ?)
                    '''
        cur.execute(statement, insertion)

    for tag in tag2:
        statement = '''
                    SELECT Id FROM Tags WHERE Tag = \"{}\"
        '''
        try:
            tagId = cur.execute(statement.format(tag)).fetchone()[0]
        except:
            continue
        insertion = (tagId, movieId, 2)
        statement = '''
                        INSERT INTO TagMovie
                        VALUES (?, ?, ?)
                    '''
        cur.execute(statement, insertion)

    conn.commit()
    conn.close()

def retrive_data1():
    insert_source()
    url = 'https://movie.douban.com/top250'
    for i in range(0, 250, 25):
        pd = '?start='
        pd += str(i)
        top_text = requests_catch(url, pd, CACHE_FNAME1)
        top_soup = BeautifulSoup(top_text, "html.parser")
        items = top_soup.find_all('div', class_='item')
        for item in items:
            item_rank = item.find('div', class_='pic').find('em').get_text()
            item_url = item.find('div', class_='hd').find('a').get('href')
            item_text = requests_catch(item_url, "", CACHE_FNAME1)
            item_soup = BeautifulSoup(item_text, 'html.parser')
            item_head = item.find('div', class_='hd').find('a').get_text(strip=True, separator="\n").split("\n")
            item_title = item_head[0]
            item_star = item.find('div', class_='star').get_text(strip=True, separator="\n").split("\n")
            item_rating = item_star[0]
            item_votes = item_star[1][:-3]
            item_tags_list = item_soup.find('div', class_='tags-body').get_text(strip=True, separator="\n").split("\n")
            # item_tags = ", ".join(item_tags_list)
            item_genre = item_soup.find_all('span', property='v:genre')
            item_genres_list = []
            for j in item_genre:
                item_genres_list.append(j.get_text())
            # item_genres = ", ".join(item_genres_list)
            item_director_list = item_soup.find('span', class_='attrs').get_text(strip=True, separator="\n").split("\n")
            while '/' in item_director_list:
                item_director_list.remove('/')
            # item_director = ", ".join(item_director_list)
            try:
                item_actors_list = item_soup.find('span', class_='actor').get_text(strip=True, separator="\n").split(
                    "\n")
                while '/' in item_actors_list:
                    item_actors_list.remove('/')
                item_actors_list = item_actors_list[2:6]
            except:
                item_actors_list = []
            # item_actors = ", ".join(item_actors_list)
            try:
                item_quote = item.find('div', class_='bd').find('p', class_='quote').get_text(strip=True)
            except:
                item_quote = "-"

            imdb_urls = item_soup.find('div', id='info').find_all('a', rel="nofollow")
            imdb_url = ''
            imdb_id = ''
            for i in imdb_urls:
                j = i.get('href')
                if j[11:15] == 'imdb':
                    imdb_url = j
                    imdb_id = j[26:]
            imdb_text = requests_catch(imdb_url, "", CACHE_FNAME2)
            imdb_soup = BeautifulSoup(imdb_text, "html.parser")
            try:
                imdb_rank = imdb_soup.find('div', id="titleAwardsRanks").find('a').get_text(strip=True)
                if imdb_rank[0] == "S":
                    imdb_rank = "-"
                else:
                    imdb_rank = imdb_rank[18:]
            except:
                imdb_rank = "-"
            imdb_quote = imdb_soup.find('div', id="titleStoryLine").find('div', class_="txt-block").get_text(strip=True,
                                                                                                             separator="\n").split(
                "\n")
            if imdb_quote[0] == "Taglines:":
                imdb_quote = imdb_quote[1]
            else:
                imdb_quote = "-"
            imdb_tags_list = imdb_soup.find('div', id="titleStoryLine").find('div', class_="see-more").get_text(
                strip=True, separator="\n").split("\n")
            while '|' in imdb_tags_list:
                imdb_tags_list.remove('|')
            imdb_tags_list = imdb_tags_list[1:-2]
            # imdb_tags = ", ".join(imdb_tags_list)
            para = {
                'i': imdb_id,
                'apikey': API_KEY
            }
            omdb_url = 'http://www.omdbapi.com/'
            imdb_dict = requests_catch(omdb_url, para, CACHE_FNAME2)
            imdb_title = imdb_dict['Title']
            imdb_director = imdb_dict['Director']
            imdb_director_list = imdb_director.split(", ")
            imdb_actors = imdb_dict['Actors']
            imdb_actors_list = imdb_actors.split(", ")
            imdb_year = imdb_dict['Year']
            imdb_runtime = imdb_dict['Runtime']
            imdb_runtime = imdb_runtime[:-4]
            imdb_genres = imdb_dict['Genre']
            imdb_genres_list = imdb_genres.split(", ")
            imdb_poster = imdb_dict['Poster']
            imdb_rating = imdb_dict['imdbRating']
            imdb_votes = imdb_dict['imdbVotes']
            imdb_languages = imdb_dict['Language']
            # imdb_languages_list = imdb_languages.split(", ")
            imdb_country = imdb_dict['Country']

            movie = Movie(imdb_title, item_title, imdb_year, imdb_runtime, imdb_country, imdb_languages, imdb_quote, item_quote, imdb_poster)
            view1 = [item_rank, item_rating, item_votes, 1, item_rank]
            view2 = [imdb_rank, imdb_rating, imdb_votes, 2, item_rank]
            actor1 = item_actors_list
            actor2 = imdb_actors_list
            director1 = item_director_list
            director2 = imdb_director_list
            genre1 = item_genres_list
            genre2 = imdb_genres_list
            tag1 = item_tags_list
            tag2 = imdb_tags_list

            insert_data1(movie, view1, view2, actor1, actor2, director1, director2, genre1, genre2, tag1, tag2)
            print(item_rank)
    print("fetch the data successfully!")

def retrive_data2():
    insert_source()
    url = 'https://movie.douban.com/top250'
    for i in range(0, 250, 25):
        pd = '?start='
        pd += str(i)
        top_text = catch_retrived(url, pd, CACHE_FNAME1)
        top_soup = BeautifulSoup(top_text, "html.parser")
        items = top_soup.find_all('div', class_='item')
        for item in items:
            item_rank = item.find('div', class_='pic').find('em').get_text()
            item_url = item.find('div', class_='hd').find('a').get('href')
            item_text = catch_retrived(item_url, "", CACHE_FNAME1)
            item_soup = BeautifulSoup(item_text, 'html.parser')
            item_tags_list = item_soup.find('div', class_='tags-body').get_text(strip=True, separator="\n").split("\n")
            # item_tags = ", ".join(item_tags_list)
            item_genre = item_soup.find_all('span', property='v:genre')
            item_genres_list = []
            for j in item_genre:
                item_genres_list.append(j.get_text())
            # item_genres = ", ".join(item_genres_list)
            item_director_list = item_soup.find('span', class_='attrs').get_text(strip=True, separator="\n").split("\n")
            while '/' in item_director_list:
                item_director_list.remove('/')
            # item_director = ", ".join(item_director_list)
            try:
                item_actors_list = item_soup.find('span', class_='actor').get_text(strip=True, separator="\n").split(
                    "\n")
                while '/' in item_actors_list:
                    item_actors_list.remove('/')
                item_actors_list = item_actors_list[2:6]
            except:
                item_actors_list = []
            # item_actors = ", ".join(item_actors_list)
            imdb_urls = item_soup.find('div', id='info').find_all('a', rel="nofollow")
            imdb_url = ''
            imdb_id = ''
            for i in imdb_urls:
                j = i.get('href')
                if j[11:15] == 'imdb':
                    imdb_url = j
                    imdb_id = j[26:]
            imdb_text = catch_retrived(imdb_url, "", CACHE_FNAME2)
            imdb_soup = BeautifulSoup(imdb_text, "html.parser")
            imdb_tags_list = imdb_soup.find('div', id="titleStoryLine").find('div', class_="see-more").get_text(
                strip=True, separator="\n").split("\n")
            while '|' in imdb_tags_list:
                imdb_tags_list.remove('|')
            imdb_tags_list = imdb_tags_list[1:-2]
            # imdb_tags = ", ".join(imdb_tags_list)
            para = {
                'i': imdb_id,
                'apikey': API_KEY
            }
            omdb_url = 'http://www.omdbapi.com/'
            imdb_dict = catch_retrived(omdb_url, para, CACHE_FNAME2)
            imdb_director = imdb_dict['Director']
            imdb_director_list = imdb_director.split(", ")
            imdb_actors = imdb_dict['Actors']
            imdb_actors_list = imdb_actors.split(", ")
            imdb_genres = imdb_dict['Genre']
            imdb_genres_list = imdb_genres.split(", ")

            actor1 = item_actors_list
            actor2 = imdb_actors_list
            director1 = item_director_list
            director2 = imdb_director_list
            genre1 = item_genres_list
            genre2 = imdb_genres_list
            tag1 = item_tags_list
            tag2 = imdb_tags_list

            insert_data2(item_rank, actor1, actor2, director1, director2, genre1, genre2, tag1, tag2)
            print(item_rank)
    print("insert data in DB successfully!")


if __name__=="__main__":
    init_db()
    retrive_data1()
    retrive_data2()

