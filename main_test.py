from test import *
import unittest


class TestWebCrawler(unittest.TestCase):
    url1 = 'https://movie.douban.com/top250'
    pd1 = '?start=0'
    top_text = requests_catch(url1, pd1, 'TEST1')
    top_soup = BeautifulSoup(top_text, "html.parser")
    items = top_soup.find_all('div', class_='item')
    item = items[0]
    item_rank = item.find('div', class_='pic').find('em').get_text()
    item_url = item.find('div', class_='hd').find('a').get('href')
    item_star = item.find('div', class_='star').get_text(strip=True, separator="\n").split("\n")
    item_rating = item_star[0]
    item_votes = item_star[1][:-3]
    item_quote = item.find('div', class_='bd').find('p', class_='quote').get_text(strip=True)
    item_text = requests_catch(item_url, "", 'TEST1')
    item_soup = BeautifulSoup(item_text, 'html.parser')
    item_genre = item_soup.find_all('span', property='v:genre')
    item_genres_list = []
    for j in item_genre:
        item_genres_list.append(j.get_text())
    item_director_list = item_soup.find('span', class_='attrs').get_text(strip=True, separator="\n").split("\n")
    while '/' in item_director_list:
        item_director_list.remove('/')
    imdb_urls = item_soup.find('div', id='info').find_all('a', rel="nofollow")
    imdb_url = ''
    imdb_id = ''
    for i in imdb_urls:
        j = i.get('href')
        if j[11:15] == 'imdb':
            imdb_url = j
            imdb_id = j[26:]

    def test_item(self):
        self.assertEqual(self.item_rank, '1')
        self.assertEqual(self.item_url, "https://movie.douban.com/subject/1292052/")
        self.assertEqual(self.item_quote, "希望让人自由。")
        self.assertEqual(self.item_rating, '9.6')

    def test_item_specific(self):
        self.assertEqual(self.item_genres_list, ['剧情', '犯罪'])
        self.assertEqual(self.item_director_list, ['弗兰克·德拉邦特'])
        self.assertEqual(self.imdb_id, 'tt0111161')

class TestOMDBapi(unittest.TestCase):
    para = {
        'i': 'tt0111161',
        'apikey': API_KEY
    }
    omdb_url = 'http://www.omdbapi.com/'
    imdb_dict = requests_catch(omdb_url, para, 'TEST2')
    imdb_title = imdb_dict['Title']
    imdb_year = imdb_dict['Year']
    imdb_runtime = imdb_dict['Runtime']
    imdb_runtime = imdb_runtime[:-4]
    imdb_genres = imdb_dict['Genre']

    def test_imdb(self):
        self.assertEqual(self.imdb_title, "The Shawshank Redemption")
        self.assertEqual(self.imdb_year, "1994")
        self.assertEqual(self.imdb_genres, "Drama")
        self.assertEqual(self.imdb_runtime, "142")


class TestDatebase(unittest.TestCase):
    try:
        conn = sqlite3.connect(DBNAME)
    except:
        print("fail to connect to database")
    cur = conn.cursor()
    def test_actormovie(self):
        statement = '''
                select am.actorid, count (*), a.name from ActorMovie as am 
                join Actors as a 
                on am.actorid = a.id 
                where am.sourceid = 2 group by am.actorid 
                order by count(*) desc
        '''
        results = self.cur.execute(statement).fetchmany(2)
        self.assertEqual(results[0][0], 13)
        self.assertEqual(results[0][1], 8)
        self.assertEqual(results[0][2], "Leslie Cheung")
        self.assertEqual(results[1][0], 29)
        self.assertEqual(results[1][1], 7)
        self.assertEqual(results[1][2], "Tom Hanks")