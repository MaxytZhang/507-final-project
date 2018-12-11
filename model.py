import sqlite3
DBNAME = 'movie.db'

def search_movie(rank):
    cur = sqlite3.connect(DBNAME)
    statement = '''
        SELECT * FROM Movies WHERE Id={}
    '''
    item = cur.execute(statement.format(rank)).fetchone()
    return item

def search_view(rank):
    cur = sqlite3.connect(DBNAME)
    statement = '''
        SELECT * FROM Views WHERE MovieId={}
    '''
    views = cur.execute(statement.format(rank)).fetchall()
    return views

def search_tag(rank, source):
    cur = sqlite3.connect(DBNAME)
    statement = '''
        select t.tag from TagMovie as tm
        join Tags as t 
        on tm.tagid = t.id 
        where tm.sourceid = {} and tm.movieid={}
    '''
    tags = cur.execute(statement.format(source, rank)).fetchall()
    return tags

def search_genre():
    return "12312312"

def search_actor_top(source):
    cur = sqlite3.connect(DBNAME)
    statement = '''
        select a.name, count(*) from ActorMovie as am 
        join Actors as a 
        on am.actorid = a.id 
        where am.sourceid = {} group by am.actorid 
        order by count(*) desc
    '''
    actors = cur.execute(statement.format(source)).fetchall()
    actor_list = []
    for actor in actors:
        actor_list.append(list(actor))
    return actor_list

def search_director_top(source):
    cur = sqlite3.connect(DBNAME)
    statement = '''
        select d.name, count(*) from DirectorMovie as dm 
        join Directors as d 
        on dm.directorid = d.id 
        where dm.sourceid = {} group by dm.directorid 
        order by count(*) desc
    '''
    directors = cur.execute(statement.format(source)).fetchall()
    director_list = []
    for director in directors:
        director_list.append(list(director))
    return director_list

def search_genre_top(source):
    cur = sqlite3.connect(DBNAME)
    statement = '''
        select g.genre, count(*) from GenreMovie as gm 
        join Genres as g 
        on gm.genreid = g.id 
        where gm.sourceid = {} group by gm.genreid 
        order by count(*) desc
        limit 10
    '''
    genres = cur.execute(statement.format(source)).fetchall()
    genre_list = []
    for genre in genres:
        genre_list.append(list(genre))
    return genre_list