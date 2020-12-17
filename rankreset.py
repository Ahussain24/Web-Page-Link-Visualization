import sqlite3

def main() :

    con = sqlite3.connect('testdb.sqlite')
    cur = con.cursor()

    #reset the ranks of all the pages having html pages NOT NULL
    cur.execute('UPDATE Pages SET new_rank = NULL WHERE html is NULL or error is NOT NULL')
    cur.execute('UPDATE Pages SET old_rank = NULL,new_rank = 1.0 WHERE html is NOT NULL AND error is NULL')
    con.commit()

    print('rank reset succuessfull')

main()
