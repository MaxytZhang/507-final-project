from flask import Flask, render_template, request, redirect, url_for, session
import model

app = Flask(__name__)

@app.route("/")

@app.route("/movie", methods=['GET', 'POST'])
def movie():
    if request.method == 'POST':
        rank = request.form["rank"]
        url = '/movie/' + str(rank)
        return redirect(url)
    else:
        return render_template('index.html')


@app.route('/movie/<rank>', methods=['GET', 'POST'])
def movie_item(rank):
    item = model.search_movie(rank)
    r = model.search_genre()
    return render_template('movie.html', item=item, words=r)


if __name__=="__main__":
    app.run(debug=True)