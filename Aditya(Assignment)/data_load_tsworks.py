import yfinance as yf
import configparser
import sqlite3

# Read the list of companies from the config file
config = configparser.ConfigParser()
config.read('config.ini')
companies = config.get('DEFAULT', 'companies').split(',')

# Connect to the SQLite database
conn = sqlite3.connect('finance_data.db')
c = conn.cursor()


# Create the finance data table if it doesn't exist
c.execute('''CREATE TABLE IF NOT EXISTS finance_data
             (company TEXT, Date DATE, open REAL, high REAL, low REAL, close REAL, volume INTEGER, PRIMARY KEY (company, Date))''')

# Download finance data for each company and insert or update it in the finance_data table
for company in companies:
    ticker = yf.Ticker(company)
    data = ticker.history(period="max")
    # Prepare the data to be inserted or updated in the finance_data table
    finance_data = []
    for index, row in data.iterrows():
        finance_data.append((company, str(index.date()), row['Open'], row['High'], row['Low'], row['Close'], row['Volume']))
    # Insert or update the finance_data in the database
    c.executemany('''INSERT INTO finance_data (company, Date, open, high, low, close, volume) 
                     VALUES (?, ?, ?, ?, ?, ?, ?) 
                     ON CONFLICT (company, Date) DO UPDATE SET open = excluded.open, high = excluded.high, low = excluded.low, close = excluded.close, volume = excluded.volume''', finance_data)
    
# Commit the changes and close the connection
conn.commit()
conn.close()
