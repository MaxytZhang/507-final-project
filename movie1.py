import requests
import json
from bs4 import BeautifulSoup

url = "https://movie.douban.com/top250"
top_text = requests.get(url).text
top_soup = BeautifulSoup(top_text, "html.parser")
# print(top_soup)
items = top_soup.find_all('div', class_='item')

for item in items:
    item_order = item.find('div', class_='pic').find('em').get_text()
    print(item_order)
    item_url = item.find('div', class_='hd').find('a').get('href')
    print(item_url)
    item_text = requests.get(item_url).text
    item_soup = BeautifulSoup(item_text, 'html.parser')
    item_tags = item_soup.find('div', class_='tags-body').get_text(strip=True, separator="\n").split("\n")
    print(item_tags)
    print("-".join(item_tags))
    item_genre = item_soup.find_all('span', property='v:genre')
    item_genres = []
    for i in item_genre:
        item_genres.append(i.get_text())
    print(item_genres)
    item_actors = item_soup.find('span', class_='actor').get_text(strip=True, separator="\n").split("\n")
    while '/' in item_actors:
        item_actors.remove('/')
    item_actors = item_actors[2:6]
    print(item_actors)
    item_director = item_soup.find('span', class_='attrs').get_text(strip=True, separator="\n").split("\n")
    while '/' in item_director:
        item_director.remove('/')
    print(item_director)
    imdb_urls = item_soup.find('div', id='info').find_all('a', rel="nofollow")
    imdb_url = ''
    imdb_id = ''
    for i in imdb_urls:
        j = i.get('href')
        if j[11:15] == 'imdb':
            imdb_url = j
            imdb_id = j[26:]
    print(imdb_url)
    print(imdb_id)
    imdb_text = requests.get(imdb_url).text
    imdb_soup = BeautifulSoup(imdb_text, "html.parser")
    imdb_star = imdb_soup.find('span', itemprop="ratingValue").get_text()
    print(imdb_star)
    imdb_people = imdb_soup.find('span', itemprop="ratingCount").get_text()
    print(imdb_people)
    imdb_rank = imdb_soup.find('div', id="titleAwardsRanks").find('a').get_text(strip=True)
    if imdb_rank[0] == "S":
        imdb_rank = None
    else:
        imdb_rank = imdb_rank[18:]
    print(imdb_rank)
    imdb_quote = imdb_soup.find('div', id="titleStoryLine").find('div', class_="txt-block").get_text(strip=True, separator="\n").split("\n")
    if imdb_quote[0] == "Taglines:":
        print(imdb_quote[1])
    print(imdb_quote)
    imdb_tags = []
    imdb_tags = imdb_soup.find('div', id="titleStoryLine").find('div', class_="see-more").get_text(strip=True, separator="\n").split("\n")
    while '|' in imdb_tags:
        imdb_tags.remove('|')
    imdb_tags = imdb_tags[1:-2]
    print(imdb_tags)
    item_head = item.find('div', class_='hd').find('a').get_text(strip=True, separator="\n").split("\n")
    item_title = item_head[0]
    print(item_title)
    item_star = item.find('div', class_='star').get_text(strip=True, separator="\n").split("\n")
    item_rating = item_star[0]
    item_votes = item_star[1][:-3]
    print(item_rating)
    print(item_votes)
    item_quote = item.find('div', class_='bd').find('p', class_='quote').get_text(strip=True)

