import sqlite3 
 
class DatabaseClass():
    def __init__(self):
        self.conn = sqlite3.connect('database.db') 
        self.cursor = self.conn.cursor() 
        self.cursor.execute(''' 
            CREATE TABLE IF NOT EXISTS sensor_data ( 
                id INTEGER PRIMARY KEY AUTOINCREMENT, 
                idSensor TEXT, 
                tipoPoluente TEXT, 
                nivel TEXT, 
                timestamp TEXT 
            ) 
        ''') 
  
    def insert_data(self,idSensor, tipoPoluente, nivel, timestamp):
        self.cursor.execute('''INSERT INTO sensor_data(idSensor, tipoPoluente, nivel, timestamp) VALUES(?,?,?,?)''', 
                    (idSensor, tipoPoluente, nivel, timestamp)) 
        self.conn.commit() 
        self.conn.close() 
        
    def last_inserted(self):
        data = self.cursor.execute('''SELECT * FROM sensor_data ORDER BY id DESC LIMIT 1;''').fetchall()[0]
        self.conn.close() 
        
        return data