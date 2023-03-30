from flask import Flask, request, jsonify
import sqlite3
import threading

app = Flask(__name__)

# Create a thread-local storage for the database connection
db_connections = threading.local()

def get_db():
    """Return the database connection for the current thread."""
    if not hasattr(db_connections, 'connection'):
        conn = sqlite3.connect('finance_data.db')
        conn.row_factory = sqlite3.Row # Allow accessing columns by name
        db_connections.connection = conn
    return db_connections.connection

@app.teardown_appcontext
def close_db(error):
    """Close the database connection at the end of the request."""
    if hasattr(db_connections, 'connection'):
        db_connections.connection.close()

@app.route('/')
def index():
    return 'The app is running!'

@app.route('/stock_data', methods=['GET'])
def get_all_stock_data():
    date = request.args.get('date')
    if not date:
        return jsonify({'error': 'Missing date parameter'}), 400
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT * FROM finance_data WHERE Date = ?', (date,))
    rows = c.fetchall()
    return jsonify([dict(row) for row in rows]), 200

@app.route('/stock_data/<company>', methods=['GET'])
def get_company_stock_data(company):
    date = request.args.get('date')
    try:
        conn = get_db()
        c = conn.cursor()
        if not date:
            c.execute('SELECT * FROM finance_data WHERE company = ?', (company,))
        else:
            c.execute('SELECT * FROM finance_data WHERE company = ? AND Date = ?', (company, date))
        rows = c.fetchall()
        return jsonify([dict(row) for row in rows]), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/stock_data/<company>', methods=['POST', 'PATCH'])
def update_company_stock_data(company):
    data = request.json
    if not data:
        return jsonify({'error': 'Missing request body'}), 400
    date = data.get('date')
    if not date:
        return jsonify({'error': 'Missing date parameter'}), 400
    open_val = data.get('open')
    high = data.get('high')
    low = data.get('low')
    close = data.get('close')
    volume = data.get('volume')
    if not open_val and not high and not low and not close and not volume:
        return jsonify({'error': 'At least one parameter to update must be provided'}), 400
    set_values = []
    if open_val:
        set_values.append(f'open = {open_val}')
    if high:
        set_values.append(f'high = {high}')
    if low:
        set_values.append(f'low = {low}')
    if close:
        set_values.append(f'close = {close}')
    if volume:
        set_values.append(f'volume = {volume}')
    set_clause = ', '.join(set_values)
    conn = get_db()
    c = conn.cursor()
    c.execute(f'UPDATE finance_data SET {set_clause} WHERE company = ? AND Date = ?', (company, date))
    if c.rowcount == 0:
        return jsonify({'error': 'No data found to update'}), 404
    conn.commit()
    return jsonify({'message': 'Data updated successfully'}), 200

@app.route('/company_stock_data/<company>', methods=['GET'])
def get_all_company_stock_data(company):
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT * FROM finance_data WHERE company = ?', (company,))
    rows = c.fetchall()
    return jsonify([dict(row) for row in rows]), 200

if __name__ == '__main__':
    app.run()
