import json
import sqlite3
from datetime import datetime


path = r"C:\Users\dhanush\Downloads\RC_2009-05"
sql_transaction = []
rows_count = 0
i = 0


#author_flair_css_class
#ups
#author
#subreddit
#downs
#gilded
#score_hidden
#name
#controversiality
#edited
#link_id
#subreddit_id
#distinguished
#body
#author_flair_text
#archived
#parent_id
#score
#id
#retrieved_on
#created_utc

connection = sqlite3.connect("reddit_database.db")
c = connection.cursor()

def create_comment_table():
    c.execute("""CREATE TABLE if not exists reddit_comment_table(body text, body_id text)""")
    

def create_table():
    c.execute("""CREATE TABLE if not exists reddit_table(subreddit text,
score int, body text, body_id int, parent_body text, parent_id text, created_time int)""")

def transaction(sql):
    global sql_transaction
    sql_transaction.append(sql)
    if len(sql_transaction) == 100:
        c.execute("BEGIN TRANSACTION")
        for s in sql_transaction:
            try:
                #print("sdf")
                c.execute(s)
            except:
                pass
        connection.commit()
        sql_transaction = []

def retriveandstorecommenttable():
    with open(path) as f:
        for row in f:
            global rows_count
            rows_count = rows_count + 1
            row = json.loads(row)
            comment = row['body']
            comment_id = row['name']
            if clean_data(comment):
                comment = format_comment(comment)
                sql = """INSERT INTO reddit_comment_table(body, body_id)
                VALUES("{}", "{}");""".format(comment, comment_id)
                transaction(sql)

            if rows_count % 1000 == 0:
                print("rows :{} time :{}".format(rows_count, str(datetime.now())))
            if rows_count == 10000:
                rows_count = 0
                break
def storemaintable():
    with open(path) as f:
        for row in f:
            global rows_count
            rows_count = rows_count + 1
            row = json.loads(row)
            subreddit = row['subreddit']
            body_id = row['name']
            score = row['score']
            parent_id = row['parent_id']
            time = row['created_utc']
            body = find_parent(body_id)
            parent_body = find_parent(parent_id)
            if parent_body and body:
                sql = """INSERT INTO reddit_table(subreddit, score, body, body_id, parent_body, parent_id,
                created_time) VALUES("{}","{}","{}","{}","{}","{}","{}"
                )""".format(subreddit, score, body, body_id, parent_body, parent_id, time)
                transaction(sql)
                
            if rows_count % 1000 == 0:
                print("main_rows :{} time{}".format(rows_count, str(datetime.now())))
            if rows_count == 10000:
                rows_count = 0
                break


def find_parent(parent_id):
    sql = "SELECT body FROM reddit_comment_table WHERE body_id = '{}'".format(parent_id)
    c.execute(sql)
    result = c.fetchone()
    if result is not None:
        return result[0]
    else:
        return False
        
    

def format_comment(comment):
    comment = comment.replace("\n", "newline").replace("\r", 'newline')
    return comment

def clean_data(comment):
    if len(comment.split(" ")) > 500 and len(comment) < 1:
        return False
    elif comment is '[removed]' and comment is '[deleted]':
        return False
    else:
        return True

def show():
    c.execute("SELECT * from reddit_comment_table")
    rows = c.fetchall()
    for row in rows:
        print(row[0], row[1])

def count():
        with open(path) as f:
            global i
            for row in f:
                row = json.loads(row)
                i=i+1
            print(i)



if __name__ == "__main__":
    #count()
    create_comment_table()
    retriveandstorecommenttable()
    create_table()
    storemaintable()
    #show()
