from flask import Flask, request, jsonify, abort, url_for
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow
import datetime
from passlib.apps import custom_app_context as pwd_context
from sqlalchemy import DateTime
from flask_cors import CORS
from flask_httpauth import HTTPBasicAuth
import psycopg2
import json
# from json_encoder import DateEncoder

auth = HTTPBasicAuth()

app = Flask(__name__)
CORS(app)

app.config.from_pyfile('config.py')

db = SQLAlchemy(app)

ma = Marshmallow(app)


class DemoOrder(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(DateTime, default=datetime.datetime.utcnow)
    account = db.Column(db.String(60))
    service = db.Column(db.String)
    message = db.Column(db.String)

    def __init__(self, account, service, message):
        self.account = account
        self.service = service
        self.message = message

    @property
    def serialize(self):
        """Return object data in easily serializable format"""
        return {
            'id': self.id,
            'date': self.date,
            'account': self.account,
            'service': self.service,
            'message': self.message
        }


class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    first_name = db.Column(db.String(32))
    last_name = db.Column(db.String(32))
    username = db.Column(db.String(32), index=True)
    email = db.Column(db.String(50))
    password_hash = db.Column(db.String(128))

    def hash_password(self, password):
        self.password_hash = pwd_context.encrypt(password)

    def verify_password(self, password):
        return pwd_context.verify(password, self.password_hash)


class DemoOrderSchema(ma.Schema):
    class Meta:
        field = ('id', 'date', 'account', 'service', 'message')


order_schema = DemoOrderSchema()
orders_schema = DemoOrderSchema(many=True)


@app.route('/order', methods=['POST'])
def add_order():
    account = request.json['account']
    service = request.json['service']
    message = request.json['message']

    new_order = DemoOrder(account, service, message)
    db.session.add(new_order)
    db.session.commit()

    return jsonify(new_order.serialize)


@app.route('/order', methods=['GET'])
def get_orders():
    all_orders = DemoOrder.query.all()
    return jsonify([order.serialize for order in all_orders])


@app.route('/order/<id>', methods=['GET'])
def get_order(id):
    order = DemoOrder.query.get(id)
    return jsonify(order.serialize)


@app.route('/users', methods=['POST'])
def new_user():
    first_name = request.json.get('first_name')
    last_name = request.json.get('last_name')
    email = request.json.get('email')
    username = request.json.get('username')
    password = request.json.get('password')
    if username is None or password is None:
        abort(400)  # missing arguments
    if User.query.filter_by(username=username).first() is not None:
        abort(400)  # existing user
    user = User(username=username, first_name=first_name, last_name=last_name, email=email)
    user.hash_password(password)
    db.session.add(user)
    db.session.commit()
    return jsonify({'username': user.username}), 201, {'Location': url_for('get_user', id=user.id, _external=True)}


@app.route('/users/<int:id>')
def get_user(id):
    user = User.query.get(id)
    if not user:
        abort(400)
    return jsonify({'username': user.username})


@app.route('/')
def index():
    return "Hello, {}!".format(auth.current_user())


@app.route('/query1', methods=['GET'])
def query1():
    author = request.args.get('author')
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    count = request.args.get('count')

    try:
        conn = psycopg2.connect(user="team15",
                                password="passw1o5rd",
                                host="142.93.163.88",
                                port="6006",
                                database="db15")
        cur = conn.cursor()
        cur.execute("SELECT author_id FROM author WHERE last_name ilike\'%{}\'".format(author))
        preprocess = cur.fetchall()
        preprocess = preprocess[0][0]

        query = f"SELECT account.customer FROM order_ INNER JOIN account ON order_.account = account.account_id " \
                "INNER JOIN service ON order_.service = service.service_id  " \
                "INNER JOIN author_to_service ON service.service_id = author_to_service.service " \
                f"WHERE author_to_service.author = {preprocess} AND order_.order_date > \'{date_from}\' AND order_.order_date < '{date_to}'" \
                "GROUP BY account.customer " \
                f"HAVING count(*) >= {count}"
        cur.execute(query)
        result_ids = cur.fetchall()
        result_ids = [i[0] for i in result_ids]
        if len(result_ids) > 0:
            if len(result_ids) > 1:
                cur.execute(f"SELECT first_name, last_name FROM customer WHERE customer_id in {tuple(result_ids)}")
            else:
                cur.execute(f"SELECT first_name, last_name FROM customer WHERE customer_id in ({result_ids[0]})")
            result = cur.fetchall()
            list_for_json = []
            for row in result:
                dict_for_json = {"first_name": row[0], "last_name": row[1]}
                list_for_json.append(dict_for_json)
            response_json = json.dumps(list_for_json)
            return response_json
        else:
            return None
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        # closing database connection.
        if (conn):
            cur.close()
            conn.close()
            print("PostgreSQL connection is closed")


@app.route('/authors', methods=['GET'])
def get_authors():
    try:
        conn = psycopg2.connect(user="team15",
                                password="passw1o5rd",
                                host="142.93.163.88",
                                port="6006",
                                database="db15")
        cur = conn.cursor()
        cur.execute("SELECT first_name, last_name FROM author")
        result = cur.fetchall()

        list_for_json = []
        for row in result:
            dict_for_json = {"first_name": row[0], "last_name": row[1]}
            list_for_json.append(dict_for_json)
        response_json = json.dumps(list_for_json)
        return response_json

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        # closing database connection.
        if (conn):
            cur.close()
            conn.close()
            print("PostgreSQL connection is closed")


@app.route('/query2', methods=['GET'])
def query2():
    customer = request.args.get('customer')
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')

    try:
        conn = psycopg2.connect(user="team15",
                                password="passw1o5rd",
                                host="142.93.163.88",
                                port="6006",
                                database="db15")
        cur = conn.cursor()
        cur.execute("SELECT customer_id FROM customer WHERE last_name=\'{}\'".format(customer))
        preprocess = cur.fetchall()
        preprocess = preprocess[0][0]

        query = "SELECT author FROM order_ " \
                "INNER JOIN account ON order_.account = account.account_id " \
                "INNER JOIN service ON order_.service = service.service_id " \
                "INNER JOIN author_to_service ON service.service_id = author_to_service.service " \
                f" WHERE customer = {preprocess} AND order_.order_date > \'{date_from}\' AND order_.order_date < \'{date_to}\'"

        cur.execute(query)
        result_ids = cur.fetchall()
        result_ids = [i[0] for i in result_ids]
        cur.execute(f"SELECT first_name, last_name FROM author WHERE author_id in {tuple(result_ids)}")
        result = cur.fetchall()
        list_for_json = []
        for row in result:
            dict_for_json = {"first_name": row[0], "last_name": row[1]}
            list_for_json.append(dict_for_json)
        response_json = json.dumps(list_for_json)
        return response_json
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        # closing database connection.
        if (conn):
            cur.close()
            conn.close()
            print("PostgreSQL connection is closed")


# 3
@app.route('/query3', methods=['GET'])
def query3():
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    count = request.args.get('count')

    try:
        conn = psycopg2.connect(user="team15",
                                password="passw1o5rd",
                                host="142.93.163.88",
                                port="6006",
                                database="db15")
        cur = conn.cursor()

        query = "SELECT * FROM " \
                "(SELECT author FROM order_ " \
                "INNER JOIN service on order_.service = service.service_id " \
                "INNER JOIN account on order_.account = account.account_id " \
                "INNER JOIN author_to_service ON service.service_id = author_to_service.service " \
                f"WHERE order_.order_date > \'{date_from}\' AND order_.order_date < \'{date_to}\' " \
                "GROUP BY author, customer)" \
                "AS FOO " \
                "GROUP BY author " \
                f"HAVING count(*) >= {count}"

        cur.execute(query)
        result_ids = cur.fetchall()
        result_ids = [i[0] for i in result_ids]
        # print(result_ids)
        cur.execute(f"SELECT first_name, last_name FROM author WHERE author_id in {tuple(result_ids)}")
        result = cur.fetchall()
        list_for_json = []
        for row in result:
            dict_for_json = {"first_name": row[0], "last_name": row[1]}
            list_for_json.append(dict_for_json)
        response_json = json.dumps(list_for_json)
        return response_json
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        # closing database connection.
        if (conn):
            cur.close()
            conn.close()
            print("PostgreSQL connection is closed")


# 4
@app.route('/query4', methods=['GET'])
def query4():
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    count = request.args.get('count')

    try:
        conn = psycopg2.connect(user="team15",
                                password="passw1o5rd",
                                host="142.93.163.88",
                                port="6006",
                                database="db15")
        cur = conn.cursor()

        query = "SELECT customer FROM order_ " \
                "INNER JOIN account on order_.account = account.account_id " \
                f"WHERE order_.order_date > \'{date_from}\' AND order_.order_date < \'{date_to}\'" \
                "GROUP BY customer "\
                f"HAVING count(*) >= {count}"

        cur.execute(query)
        result_ids = cur.fetchall()
        result_ids = [i[0] for i in result_ids]
        # print(result_ids)
        cur.execute(f"SELECT first_name, last_name FROM customer WHERE customer_id in {tuple(result_ids)}")
        result = cur.fetchall()
        list_for_json = []
        for row in result:
            dict_for_json = {"first_name": row[0], "last_name": row[1]}
            list_for_json.append(dict_for_json)
        response_json = json.dumps(list_for_json)
        return response_json
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        # closing database connection.
        if (conn):
            cur.close()
            conn.close()
            print("PostgreSQL connection is closed")


# 5
@app.route('/query5', methods=['GET'])
def query5():
    customer = request.args.get('customer')
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    count = request.args.get('count')

    try:
        conn = psycopg2.connect(user="team15",
                                password="passw1o5rd",
                                host="142.93.163.88",
                                port="6006",
                                database="db15")
        cur = conn.cursor()
        cur.execute("SELECT customer_id FROM customer WHERE last_name=\'{}\'".format(customer))
        preprocess = cur.fetchall()
        preprocess = preprocess[0][0]

        query = "SELECT social_network FROM order_ " \
                "INNER JOIN account on order_.account = account.account_id " \
                f"WHERE customer = {preprocess} AND order_.order_date > \'{date_from}\' AND order_.order_date < \'{date_to}\' " \
                "GROUP BY social_network " \
                f"HAVING count(*) >= {count}"
        cur.execute(query)
        result = cur.fetchall()
        list_for_json = []
        for row in result:
            dict_for_json = {"social_network": row[0]}
            list_for_json.append(dict_for_json)
        response_json = json.dumps(list_for_json)
        return response_json
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        # closing database connection.
        if (conn):
            cur.close()
            conn.close()
            print("PostgreSQL connection is closed")


# 8
@app.route('/query8', methods=['GET'])
def query8():
    author = request.args.get('author')
    customer = request.args.get('customer')
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')

    try:
        conn = psycopg2.connect(user="team15",
                                password="passw1o5rd",
                                host="142.93.163.88",
                                port="6006",
                                database="db15")
        cur = conn.cursor()
        cur.execute("SELECT author_id FROM author WHERE last_name=\'{}\'".format(author))
        preprocess_a = cur.fetchall()
        preprocess_a = preprocess_a[0][0]

        cur.execute("SELECT customer_id FROM customer WHERE last_name=\'{}\'".format(customer))
        preprocess_c = cur.fetchall()
        preprocess_c = preprocess_c[0][0]

        query = "SELECT order_id FROM order_" \
                "INNER JOIN account ON order_.account = account.account_id" \
                "INNER JOIN service ON order_.service = service.service_id" \
                "INNER JOIN author_to_service ON service.service_id = author_to_service.service" \
                f"WHERE author = {preprocess_a} AND customer = {preprocess_c} AND order_.order_date > \'{date_from}\' AND order_.order_date < \'{date_to}\'"

        cur.execute(query)
        result_ids = cur.fetchall()
        result_ids = [i[0] for i in result_ids]
        # print(result_ids)
        cur.execute(f"SELECT order_date FROM order WHERE order_id in {tuple(result_ids)}")
        result = cur.fetchall()
        list_for_json = []
        for i in result:
            list_for_json.append(i)
        response_json = json.dumps(list_for_json)
        return response_json
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        # closing database connection.
        if (conn):
            cur.close()
            conn.close()
            print("PostgreSQL connection is closed")


# 10
@app.route('/query10', methods=['GET'])
def query10():
    customer = request.args.get('customer')
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')

    try:
        conn = psycopg2.connect(user="team15",
                                password="passw1o5rd",
                                host="142.93.163.88",
                                port="6006",
                                database="db15")
        cur = conn.cursor()
        cur.execute("SELECT customer_id FROM customer WHERE last_name=\'{}\'".format(customer))
        preprocess = cur.fetchall()
        preprocess = preprocess[0][0]

        query = "SELECT count(*) FROM order_" \
                "INNER JOIN account ON order_.account = account.account_id" \
                "INNER JOIN service ON order_.service = service.service_id" \
                "INNER JOIN author_to_service on service.service_id = author_to_service.service" \
                "INNER JOIN author ON author_to_service.author = author.author_id" \
                "INNER JOIN sale_by_author ON author.author_id = sale_by_author.author AND service.style = sale_by_author.style" \
                f"WHERE customer = {preprocess} AND order_.order_date > \'{date_from}\' AND order_.order_date < \'{date_to}\'" \
                "AND discount >= 50 AND start_date < order_.order_date AND end_date > order_.order_date"

        cur.execute(query)
        result = cur.fetchall()
        list_for_json = []
        for i in result:
            list_for_json.append(i)
        response_json = json.dumps(list_for_json)
        return response_json
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        # closing database connection.
        if (conn):
            cur.close()
            conn.close()
            print("PostgreSQL connection is closed")


# 11
@app.route('/query11', methods=['GET'])
def query11():
    try:
        conn = psycopg2.connect(user="team15",
                                password="passw1o5rd",
                                host="142.93.163.88",
                                port="6006",
                                database="db15")
        cur = conn.cursor()

        query = "SELECT date_trunc('month', order_.order_date) as month, count(*) FROM order_" \
                "GROUP BY month"

        cur.execute(query)
        result_ids = cur.fetchall()
        result_ids = [i[0] for i in result_ids]
        # print(result_ids)
        cur.execute(f"SELECT first_name, last_name FROM customer WHERE customer_id in {tuple(result_ids)}")
        result = cur.fetchall()
        list_for_json = []
        for i in result:
            list_for_json.append(i)
        response_json = json.dumps(list_for_json)
        return response_json
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        # closing database connection.
        if (conn):
            cur.close()
            conn.close()
            print("PostgreSQL connection is closed")


if __name__ == '__main__':
    app.run(debug=True)
