from flask import render_template

def register_routes(app):
    @app.route("/")
    def dashboard():
        return render_template("dashboard.html")

    @app.route("/transaction-logs")
    def transaction_logs():
        return render_template("transaction-logs.html")
    
    @app.route("/create")
    def create():
        return render_template("create-title.html")
    
    @app.route("/browse")
    def search():
        return render_template("title-browser.html")