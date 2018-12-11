from flask import Flask, render_template, request, redirect, url_for, session
import model

app = Flask(__name__)

@app.route("/", methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        rank = request.form["rank"]
        url = '/movie/' + str(rank)
        return redirect(url)
    else:
        return render_template('index.html')


@app.route('/movie/<rank>', methods=['GET', 'POST'])
def movie(rank):
    item = model.search_movie(rank)
    view = model.search_view(rank)
    tag1 = model.search_tag(rank,1)
    tag2 = model.search_tag(rank,2)
    return render_template('movie.html', item=item, view=view, tag1=tag1, tag2=tag2)

@app.route('/actor', methods=['GET', 'POST'])
def actor():
    actors1 = model.search_actor_top(1)
    actors2 = model.search_actor_top(2)
    return render_template('actor.html', actors1=actors1, actors2=actors2)

@app.route('/director', methods=['GET', 'POST'])
def director():
    directors1 = model.search_director_top(1)
    directors2 = model.search_director_top(2)
    return render_template('director.html', directors1=directors1, directors2=directors2)

@app.route('/genre', methods=['GET', 'POST'])
def genre():
    genres1 = model.search_genre_top(1)
    genres2 = model.search_genre_top(2)
    return render_template('genre.html', genres1=genres1, genres2=genres2)


if __name__=="__main__":
    app.run(debug=True)