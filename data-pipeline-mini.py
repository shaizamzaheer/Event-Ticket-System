#%%
import logging
import mysql.connector
import configparser

config = configparser.ConfigParser()
config.read('config.ini')
username  = config['DEFAULT']['username']
password = config['DEFAULT']['password']
host = config['DEFAULT']['host']

stmt = """
CREATE TABLE IF NOT EXISTS sales (
ticket_id INT,
trans_date DATE,
event_id INT,
event_name VARCHAR(50),
event_date DATE,
event_type VARCHAR(10),
event_city VARCHAR(20),
customer_id INT,
price DECIMAL,
num_tickets INT)
"""

def get_db_connection() -> None:
    connection = None
    try:
        connection = mysql.connector.connect(
        user = username,
        password = password,
        host = host,
        port = '3306',
        database = 'TICKET_SALES')
    except Exception as error:
        print("Error while connecting to database for job tracker", error)

    return connection

def load_third_party(connection, file_path_csv):
    cursor = connection.cursor()
    cursor.execute('DROP TABLE IF EXISTS sales;')
    cursor.execute(stmt)
    with open(file_path_csv, 'r') as csv_data:
        for row in csv_data:
            row = row.split(',')
            sql = "INSERT INTO TICKET_SALES.sales VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, [row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9]])
    connection.commit()
    cursor.close()
    return

def query_popular_tickets(connection):
    # Get the most popular ticket in the past month
    sql_statement = """
    SELECT event_name 
    from sales 
    where
    trans_date >= (select max(trans_date)- INTERVAL 1 MONTH from sales)
    group by event_name
    """

    cursor = connection.cursor()
    cursor.execute(sql_statement)
    records = cursor.fetchall()
    cursor.close()
    return list(zip(*records))
    

def main():
    logging.basicConfig(filename='status.log', encoding='utf-8', level=logging.DEBUG)
    conn = get_db_connection()
    load_third_party(conn,'third_party_sales.csv')
    res = query_popular_tickets(conn)
    return res

if __name__ == '__main__':
    recs = main()
    print("Here are the most popular tickets in the past month:")
    for rec in recs[0]:
        print(f"- {rec}")

