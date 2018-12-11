import sqlite3
DBNAME = 'movie.db'

def search_movie(rank):
    cur = sqlite3.connect(DBNAME)
    statement = '''
        SELECT * FROM Movies WHERE Id={}
    '''
    item = cur.execute(statement.format(rank)).fetchone()
    return item

def search_genre():
    return "12312312"

search_movie(1)