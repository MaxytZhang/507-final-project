import requests
import json
from bs4 import BeautifulSoup
import secret
import sqlite3

API_KEY = secret.api_key
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
            'ChiQuote' TEXT NOT NULL
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
                'SourceId' INTEGER NOT NULL
        );
    '''
    cur.execute(statement)

    statement = '''
        CREATE TABLE 'Directors' (
                'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
                'Name' TEXT,
                'SourceId' INTEGER NOT NULL
        );
    '''
    cur.execute(statement)

    statement = '''
        CREATE TABLE 'Genres' (
                'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
                'Genre' TEXT,
                'SourceId' INTEGER NOT NULL
        );
    '''
    cur.execute(statement)

    statement = '''
        CREATE TABLE 'Tags' (
                'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
                'Tag' TEXT,
                'SourceId' INTEGER NOT NULL
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

def insert_data1():
    try:
        conn = sqlite3.connect(DBNAME)
    except:
        print("fail to connect to database")
        return
    cur = conn.cursor()
    with open("flavors_of_cacao_cleaned.csv", 'r') as f:
        frows = csv.reader(f)
        first_row = 0
        for row in frows:
            first_row += 1
            if first_row == 1:
                continue
            try:
                cid = cur.execute("SELECT Id FROM Countries WHERE EnglishName = ?", (row[5],)).fetchone()[0]
            except:
                cid = None
            try:
                bid = cur.execute("SELECT Id FROM Countries WHERE EnglishName = ?", (row[8],)).fetchone()[0]
            except:
                bid = None
            insertion = (None, row[0], row[1], row[2], row[3], float(row[4][:-1])/100, cid, row[6], row[7], bid)
            statement = '''
                        INSERT INTO Bars
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    '''
            cur.execute(statement, insertion)
    conn.commit()
    conn.close()

def retrive_data():
    url = 'https://movie.douban.com/top250'
    for i in range(0, 250, 25):
        pd = '?start='
        pd += str(i)
        top_text = requests_catch(url, pd, CACHE_FNAME1)
        top_soup = BeautifulSoup(top_text, "html.parser")
        items = top_soup.find_all('div', class_='item')
        for item in items:
            movie = []
            view = []
            actor = []
            director = []
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
            item_tags = ", ".join(item_tags_list)
            item_genre = item_soup.find_all('span', property='v:genre')
            item_genres_list = []
            for j in item_genre:
                item_genres_list.append(j.get_text())
            item_genres = ", ".join(item_genres_list)
            item_director_list = item_soup.find('span', class_='attrs').get_text(strip=True, separator="\n").split("\n")
            while '/' in item_director_list:
                item_director_list.remove('/')
            item_director = ", ".join(item_director_list)
            try:
                item_actors_list = item_soup.find('span', class_='actor').get_text(strip=True, separator="\n").split(
                    "\n")
                while '/' in item_actors_list:
                    item_actors_list.remove('/')
                item_actors_list = item_actors_list[2:6]
            except:
                item_actors_list = []
            item_actors = ", ".join(item_actors_list)
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
            imdb_tags = ", ".join(imdb_tags_list)
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
            imdb_languages_list = imdb_languages.split(", ")
            imdb_country = imdb_dict['Country']

            item_year = imdb_year
            item_runtime = imdb_runtime
            item_languages = imdb_languages
            item_languages_list = imdb_languages_list
            item_country = imdb_country

            # print(item_url)
            # print(item_rank)
            # print(item_rating)
            # print(item_votes)
            # print(item_title)
            # print(item_year)
            # print(item_runtime)
            # print(item_country)
            # print(item_languages)
            # print(item_languages_list)
            # print(item_director)
            # print(item_director_list)
            # print(item_actors)
            # print(item_actors_list)
            # print(item_genres)
            # print(item_genres_list)
            # print(item_quote)
            # print(item_tags)
            # print(item_tags_list)
            # print("*"*10)
            # print(imdb_url)
            # print(imdb_rank)
            # print(imdb_rating)
            # print(imdb_votes)
            # print(imdb_title)
            # print(imdb_year)
            # print(imdb_runtime)
            # print(imdb_country)
            # print(imdb_languages)
            # print(imdb_languages_list)
            # print(imdb_director)
            # print(imdb_director_list)
            # print(imdb_actors)
            # print(imdb_actors_list)
            # print(imdb_genres)
            # print(imdb_genres_list)
            # print(imdb_quote)
            # print(imdb_tags)
            # print(imdb_tags_list)
            # print("*"*10)


if __name__=="__main__":
    init_db()
    retrive_data()

