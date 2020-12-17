import sqlite3
import urllib.parse
from urllib.request import urlopen
from bs4 import BeautifulSoup

def create(cur):

    #cur.executescript('''
    #DROP TABLE IF EXISTS Pages;
    #DROP TABLE IF EXISTS Links
    #''')
    #cur.execute(''' CREATE TABLE IF NOT EXISTS Websites(
    #web TEXT UNIQUE)
    #''')

    cur.execute('''CREATE TABLE IF NOT EXISTS Pages(
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    url TEXT UNIQUE,
    html TEXT,
    error INTEGER,
    old_rank INTEGER,
    new_rank REAL
    )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS Links(
    from_id INTEGER,
    to_id INTEGER
    )''')

#Find links that are internel and free of all unneccessary details
def gatherlinks(anchor_tags) :

    hostname = 'https://www.google.org'
    link = list()
    for tags in anchor_tags :
        try :
            tag = tags.get('href')
            if tag is None :
                continue
            if not (tag.find('#')):
                continue
            if not ( urllib.parse.urlparse(tag).scheme == 'https' or urllib.parse.urlparse(tag).scheme == 'http' ) :
                continue
            if not (urllib.parse.urlparse(tag).netloc.split('.')[1] == urllib.parse.urlparse(hostname).netloc.split('.')[1] ):
                continue
            if tag.endswith('.png') or tag.endswith('.jpg') or tag.endswith('.jpeg') or tag.endswith('.mp3') or tag.endswith('.mp4') or tag.endswith('.mkv') :
                continue
            if tag.endswith('/'):
                tag = tag[:len(tag) - 1]
            link.append(tag)
        except :
            continue
    return link

#connect to the supplied url and if everythings ok return response elese return None

def pageDoesNotExists(con,cur,url,fromID) :

    cur.execute('DELETE from Links WHERE from_id=?', (fromID, ) )
    print('fetching ',url)
    try :
        r = urlopen(url)
        if  r.getcode() == 200  and r.info().get_content_type() == 'text/html' :
            try :
                c = r.read()
                soup = BeautifulSoup(c,"html.parser")
                cur.execute('INSERT OR IGNORE INTO Pages(url, html, new_rank) VALUES ( ?, NULL, 1.0 )', ( url, ) )
                cur.execute('UPDATE Pages SET html=? WHERE url=?', (memoryview(c), url) )
                con.commit()
                cur.execute('SELECT id FROM Pages WHERE url = ?',(url,))
                toID = cur.fetchone()[0]
                cur.execute('INSERT INTO Links(from_id,to_id) VALUES(?,?)',(fromID,toID))
                con.commit()
                links = gatherlinks(soup.find_all('a'))
                #print(links)
                fromID = toID
                toID = None
                for link in links :
                        cur.execute('INSERT OR IGNORE INTO Pages (url,html,new_rank) VALUES ( ?, NULL,1.0 )', ( link, ) )
                        con.commit()
                        cur.execute('SELECT id FROM Pages WHERE url = ?',(link,))
                        toID = cur.fetchone()[0]
                        cur.execute('INSERT INTO Links VALUES(?,?)',(fromID,toID))

            except :
                cur.execute('UPDATE Pages SET url = ?,html = NULL ',(url,))
                con.commit()

        else :
            print('Unbale to retrieve ',r.getcode() == 200)
            cur.execute('UPDATE Pages SET error = ? WHERE url=?',(r.getcode() == 200,url) )
            con.commit()
    except :
        print('Some error occurred')
    return

#crawl a said website
def crawl(cur,con,n) :

    i = 0
    while n :
        cur.execute('SELECT  id,url FROM Pages WHERE html is NULL AND error is NULL ORDER BY RANDOM() LIMIT 1')
        row = cur.fetchone()
        if  row is None :
            print('No unretrieved page found',row)
            quit()
        else:
            fromID = row[0]
            url = row[1]
            pageDoesNotExists(con,cur,url,fromID)


def main() :
    con = sqlite3.connect('testdb2.sqlite')
    cur = con.cursor()
    create(cur)
    url = 'https://www.google.org'
    #cur.execute('INSERT INTO Websites VALUES(?)',(url,))
    cur.execute('INSERT OR IGNORE INTO Pages (url, html,new_rank) VALUES ( ?,NULL,1.0 )',(url,))
    con.commit()

    #cur.execute('SELECT  id,url FROM Pages WHERE html is NULL AND error is NULL ORDER BY RANDOM() LIMIT 1')
    #row = cur.fetchone()
    #print(row)
    print('Starting fresh crawl..')
    #crawl the site
    crawl(cur,con,5)
    print('Done fetching')




main()
