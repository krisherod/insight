from app import app

@app.route('\t')
@app.route('/index/')

def index():
	return "Hwoll"