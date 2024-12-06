import sqlite3
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, render_template_string
from datetime import datetime
import json

class Node:
    def __init__(self, is_leaf=False):
        self.is_leaf = is_leaf
        self.keys = []
        self.children = []
        self.next_leaf = None  # Link to next leaf node for range queries

class BPlusTree:
    def __init__(self, max_keys=4):
        self.root = Node(is_leaf=True)
        self.max_keys = max_keys

    def insert(self, key, value):
        root = self.root
        if len(root.keys) == self.max_keys:
            new_root = Node()
            new_root.children.append(self.root)
            self.split_child(new_root, 0)
            self.root = new_root
        self.insert_non_full(self.root, key, value)

    def insert_non_full(self, node, key, value):
        if node.is_leaf:
            index = 0
            # Extract just the timestamp for comparison
            while index < len(node.keys) and key > node.keys[index][0]:
                index += 1
            node.keys.insert(index, (key, value))
        else:
            index = 0
            # Extract just the timestamp for comparison
            while index < len(node.keys) and key > node.keys[index][0]:
                index += 1
            if len(node.children[index].keys) == self.max_keys:
                self.split_child(node, index)
                # Extract just the timestamp for comparison
                if key > node.keys[index][0]:
                    index += 1
            self.insert_non_full(node.children[index], key, value)


    def split_child(self, parent, index):
        node_to_split = parent.children[index]
        mid_index = len(node_to_split.keys) // 2
        split_key = node_to_split.keys[mid_index][0]  # Use only the datetime object as the key

        left_child = Node(is_leaf=node_to_split.is_leaf)
        right_child = Node(is_leaf=node_to_split.is_leaf)

        left_child.keys = node_to_split.keys[:mid_index]
        right_child.keys = node_to_split.keys[mid_index + 1:]

        if not node_to_split.is_leaf:
            left_child.children = node_to_split.children[:mid_index + 1]
            right_child.children = node_to_split.children[mid_index + 1:]
        else: 
            # Maintain next_leaf pointers for range queries
            left_child.next_leaf = right_child
            right_child.next_leaf = node_to_split.next_leaf

        parent.keys.insert(index, (split_key, None))  # Store split key with None as placeholder for value
        parent.children[index] = left_child
        parent.children.insert(index + 1, right_child)


    def range_query(self, start_key, end_key):
        result = []
        self._range_query(self.root, start_key, end_key, result)
        return result

    def _range_query(self, node, start_key, end_key, result):
        if node.is_leaf:
            for key, value in node.keys:
                if start_key <= key <= end_key:
                    result.append((key, value))
        else:
            for i in range(len(node.keys)):
                if start_key <= node.keys[i][0]:  # Extract the timestamp for comparison
                    self._range_query(node.children[i], start_key, end_key, result)
            # Search the last child if the end key is greater than the last key
            if node.keys and end_key > node.keys[-1][0]:  # Extract the timestamp for comparison
                self._range_query(node.children[-1], start_key, end_key, result)

class TimeSeriesDatabase:
    def __init__(self, db_name="climate_data.db"):
        self.conn = sqlite3.connect(db_name, check_same_thread=False)
        self.create_tables()
        self.bptree = BPlusTree()
        self.last_timestamp = None

    def create_tables(self):
        """
        Create the necessary tables to store time-series data and support indexing.
        """
        create_data_table = '''
        CREATE TABLE IF NOT EXISTS climate_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            delta_timestamp INTEGER NOT NULL,
            station_id TEXT NOT NULL,
            metric_name TEXT NOT NULL,
            value REAL NOT NULL,
            location TEXT,
            tags TEXT
        );
        '''
        
        cursor = self.conn.cursor()
        cursor.execute(create_data_table)
        self.conn.commit()

    def insert_data(self, timestamp, station_id, metric_name, value, location=None, tags=None):
        """
        Insert a new data point into the climate_data table and B+-tree index.
        """
        timestamp_dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')

        if self.last_timestamp is None:
            delta_timestamp = 0  # First data point
        else:
            delta_timestamp = int((timestamp_dt - self.last_timestamp).total_seconds() * 1000)  # Convert to milliseconds

        self.last_timestamp = timestamp_dt

        insert_query = '''
        INSERT INTO climate_data (delta_timestamp, station_id, metric_name, value, location, tags)
        VALUES (?, ?, ?, ?, ?, ?);
        '''

        cursor = self.conn.cursor()
        cursor.execute(insert_query, (delta_timestamp, station_id, metric_name, value, location, tags))
        self.conn.commit()

        # Get the record ID of the inserted row
        record_id = cursor.lastrowid

        # Insert only the timestamp and record ID into the B+-Tree
        self.bptree.insert(self.last_timestamp, record_id)


    def retrieve_data(self, start_time, end_time, station_id=None):
        """
        Retrieve data over a specific time interval using B+-tree for range queries.
        Optionally filter by station_id.
        """

        # Ensure inputs are sanitized
        start_time_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        end_time_dt = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')

        # Use B+-Tree to get the list of record IDs that fall within the time range
        result = self.bptree.range_query(start_time_dt, end_time_dt)
        record_ids = [entry[1] for entry in result]  # Extract record IDs from B+-tree result

        if not record_ids:
            return []

        # Prepare the SQL query to retrieve records by IDs
        placeholders = ', '.join(['?'] * len(record_ids))
        select_query = f'''
        SELECT * FROM climate_data
        WHERE id IN ({placeholders})
        '''

        cursor = self.conn.cursor()
        cursor.execute(select_query, record_ids)
        records = cursor.fetchall()

        # Optional filtering by station_id
        if station_id:
            records = [record for record in records if record[2] == station_id]  # Assuming station_id is in the 3rd column

        return records


    def update_data(self, record_id, value):
        """
        Update the value of a specific record.
        """
        update_query = '''
        UPDATE climate_data
        SET value = ?
        WHERE id = ?;
        '''
        
        cursor = self.conn.cursor()
        cursor.execute(update_query, (value, record_id))
        self.conn.commit()

    def delete_data(self, record_id):
        """
        Delete a specific record from the climate_data table.
        """
        delete_query = '''
        DELETE FROM climate_data
        WHERE id = ?;
        '''
        
        cursor = self.conn.cursor()
        cursor.execute(delete_query, (record_id,))
        self.conn.commit()

    def aggregate_data(self, start_time, end_time, metric_name, aggregation_type="avg"):
        """
        Perform aggregation queries over a specific time interval.
        """
        if aggregation_type not in ["avg", "sum", "min", "max"]:
            raise ValueError("Invalid aggregation type. Choose from 'avg', 'sum', 'min', 'max'.")

        start_time_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        end_time_dt = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
        delta_start = int((start_time_dt - self.last_timestamp).total_seconds() * 1000)  # Convert to milliseconds
        delta_end = int((end_time_dt - self.last_timestamp).total_seconds() * 1000)  # Convert to milliseconds

        aggregate_query = f'''
        SELECT {aggregation_type}(value)
        FROM climate_data
        WHERE delta_timestamp BETWEEN ? AND ? AND metric_name = ?;
        '''

        cursor = self.conn.cursor()
        cursor.execute(aggregate_query, (delta_start, delta_end, metric_name))
        result = cursor.fetchone()[0]

        return result if result is not None else 0.0


    def downsample_data(self, start_time, end_time, metric_name, interval='hourly'):
        """
        Downsample data by a specific time interval (e.g., hourly, daily).
        Return aggregated values for each interval.
        """
        if interval not in ['hourly', 'daily']:
            raise ValueError("Invalid interval type. Choose from 'hourly', 'daily'.")

        if interval == 'hourly':
            time_format = '%Y-%m-%d %H:00:00'
            step = timedelta(hours=1)
        elif interval == 'daily':
            time_format = '%Y-%m-%d 00:00:00'
            step = timedelta(days=1)

        current_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        end_time_dt = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
        result = []

        while current_time <= end_time_dt:
            next_time = current_time + step
            aggregate_value = self.aggregate_data(current_time.strftime('%Y-%m-%d %H:%M:%S'),
                                                next_time.strftime('%Y-%m-%d %H:%M:%S'),
                                                metric_name, aggregation_type='avg')
            result.append({
                'timestamp': current_time.strftime(time_format),
                'value': aggregate_value
            })
            current_time = next_time

        return result


    def close(self):
        """
        Close the database connection.
        """
        self.conn.close()

# Flask API to interact with the TimeSeriesDatabase
db = TimeSeriesDatabase()
app = Flask(__name__)


@app.route('/insert', methods=['POST'])
def insert_data():
    data = request.get_json()
    timestamp = data['timestamp']
    station_id = data['station_id']
    metric_name = data['metric_name']
    value = data['value']
    location = data.get('location')
    tags = data.get('tags')
    
    db.insert_data(timestamp, station_id, metric_name, value, location, tags)
    return jsonify({"message": "Data inserted successfully"})

@app.route('/retrieve', methods=['GET'])
def retrieve_data():
    start_time = request.args.get('start_time')
    end_time = request.args.get('end_time')
    station_id = request.args.get('station_id')
    
    result = db.retrieve_data(start_time, end_time, station_id)
    return jsonify(result)

@app.route('/update', methods=['PUT'])
def update_data():
    data = request.get_json()
    record_id = data['record_id']
    value = data['value']
    
    db.update_data(record_id, value)
    return jsonify({"message": "Data updated successfully"})

@app.route('/delete', methods=['DELETE'])
def delete_data():
    record_id = request.args.get('record_id')
    
    db.delete_data(record_id)
    return jsonify({"message": "Data deleted successfully"})

@app.route('/aggregate', methods=['GET'])
def aggregate_data():
    start_time = request.args.get('start_time')
    end_time = request.args.get('end_time')
    metric_name = request.args.get('metric_name')
    aggregation_type = request.args.get('aggregation_type', 'avg')
    
    result = db.aggregate_data(start_time, end_time, metric_name, aggregation_type)
    return jsonify({"aggregation_result": result})

@app.route('/downsample', methods=['GET'])
def downsample_data():
    start_time = request.args.get('start_time')
    end_time = request.args.get('end_time')
    metric_name = request.args.get('metric_name')
    interval = request.args.get('interval', 'hourly')
    
    result = db.downsample_data(start_time, end_time, metric_name, interval)
    return jsonify(result)

@app.route('/', methods=['GET', 'POST'])
def home():
    result = None
    if request.method == 'POST':
        start_time = request.form.get('start_time')
        end_time = request.form.get('end_time')
        station_id = request.form.get('station_id')
        result = db.retrieve_data(start_time, end_time, station_id)
    
    page_content = '''
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Time Series Database API</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                line-height: 1.6;
                margin: 0;
                padding: 20px;
                background-color: #f4f4f4;
                color: #333;
            }
            .container {
                max-width: 800px;
                margin: 0 auto;
                padding: 20px;
                background: #fff;
                border-radius: 8px;
                box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
            }
            h1 {
                text-align: center;
            }
            pre {
                white-space: pre-wrap; /* Allows long text to wrap */
                word-wrap: break-word; /* Breaks long words to avoid overflow */
                background: #f8f8f8;
                padding: 10px;
                border-radius: 5px;
                overflow-x: auto;
                max-width: 100%;
            }
            form {
                display: flex;
                flex-direction: column;
            }
            label {
                margin-top: 10px;
            }
            input[type="text"], input[type="submit"] {
                padding: 10px;
                margin-top: 5px;
                border-radius: 5px;
                border: 1px solid #ccc;
            }
            input[type="submit"] {
                background-color: #007bff;
                color: #fff;
                cursor: pointer;
                transition: background-color 0.3s;
            }
            input[type="submit"]:hover {
                background-color: #0056b3;
            }
            ul {
                list-style-type: none;
                padding: 0;
            }
            li {
                margin: 5px 0;
            }
            code {
                background-color: #e8e8e8;
                padding: 2px 4px;
                border-radius: 4px;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Welcome to the Time Series Database API!</h1>
            <p>Available Endpoints:</p>
            <ul>
                <li><code>/insert</code> - Insert data (POST)</li>
                <li><code>/update</code> - Update data (PUT)</li>
                <li><code>/retrieve?start_time=&lt;start&gt;&end_time=&lt;end&gt;</code> - Retrieve data (GET)</li>
                <li><code>/delete</code> - Delete data (DELETE)</li>
                <li><code>/aggregate</code> - Aggregate data (GET)</li>
                <li><code>/downsample</code> - Downsample data (GET)</li>
            </ul>
            <h2>Retrieve Data</h2>
            <form method="POST">
                <label for="start_time">Start Time (YYYY-MM-DD HH:MM:SS):</label>
                <input type="text" id="start_time" name="start_time" required>
                
                <label for="end_time">End Time (YYYY-MM-DD HH:MM:SS):</label>
                <input type="text" id="end_time" name="end_time" required>
                
                <label for="station_id">Station ID:</label>
                <input type="text" id="station_id" name="station_id">
                
                <input type="submit" value="Retrieve Data">
            </form>
            {% if result %}
                <h3>Query Results:</h3>
                <pre>{{ result }}</pre>
            {% endif %}
            <p>Use these endpoints with appropriate parameters to interact with the database.</p>
        </div>
    </body>
    </html>
    '''

    return render_template_string(page_content, result=result)

if __name__ == '__main__':
    app.run(debug=True)

# Example usage
db.insert_data(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "Station_1", "temperature", 22.5, "New York", "urban,high-altitude")
db.insert_data(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "Station_1", "humidity", 60.0, "New York", "urban,high-altitude")
