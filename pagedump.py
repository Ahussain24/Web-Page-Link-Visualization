import sqlite3

def main() :

    con = sqlite3.connect('testdb.sqlite')
    cur = con.cursor()

    #select all the pages with the number of links pointing to them
    cur.execute('''SELECT Pages.id ,Pages.url,COUNT(from_id) AS inbound FROM 
    Links JOIN Pages ON links.to_id = Pages.id WHERE Pages.html is NOT NULL GROUP BY to_id ;
     ''')

    print("{:<5} {:<25} {:<10}".format('ID','URL','INBOUND'))

    for item in cur :
        if not item is None :
           print("{:<5} {:<20} {:<30}".format(item[0],item[1],item[2]))
    
main()
