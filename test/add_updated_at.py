import os
import mysql.connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
MYSQL_PORT = os.getenv('MYSQL_PORT', '3306')
MYSQL_DB = os.getenv('MYSQL_DB', 'etl_db')
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', '1')

def add_updated_at_column():
    try:
        # Connect to MySQL
        print(f"Connecting to MySQL at {MYSQL_HOST}...")
        cnx = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB
        )
        cursor = cnx.cursor()

        # 1. Check if the column exists
        print("Checking if 'updated_at' column exists in 'events' table...")
        cursor.execute("""
            SELECT COUNT(*) 
            FROM information_schema.columns 
            WHERE table_schema = %s 
            AND table_name = 'events' 
            AND column_name = 'updated_at'
        """, (MYSQL_DB,))
        
        column_exists = cursor.fetchone()[0]

        if column_exists == 0:
            # 2. Add the column
            print("Adding 'updated_at' column...")
            alter_query = """
                ALTER TABLE events 
                ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            """
            cursor.execute(alter_query)
            print("Column 'updated_at' added successfully with automatic timestamps.")
        else:
            print("Column 'updated_at' already exists.")

        # 3. Update existing null values (optional, but good for consistency)
        print("Ensuring all records have an 'updated_at' value...")
        update_query = "UPDATE events SET updated_at = CURRENT_TIMESTAMP WHERE updated_at IS NULL"
        cursor.execute(update_query)
        
        cnx.commit()
        print("Database update completed.")

        cursor.close()
        cnx.close()

    except mysql.connector.Error as err:
        print(f"Error: {err}")

if __name__ == "__main__":
    add_updated_at_column()
