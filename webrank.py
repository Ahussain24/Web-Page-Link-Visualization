import sqlite3

con = sqlite3.connect('testdb2.sqlite')
cur = con.cursor()

# Find the ids that send out page rank - we only are interested
# in pages in the SCC that have in and out links
cur.execute('''SELECT DISTINCT from_id FROM Links''')
from_ids = list()
for row in cur:
    from_ids.append(row[0])

# Find the ids that receive page rank
to_ids = list()
links = list()
cur.execute('''SELECT DISTINCT from_id, to_id FROM Links''')
for row in cur:
    from_id = row[0]
    to_id = row[1]
    if from_id == to_id : continue
    if from_id not in from_ids : continue
    if to_id not in from_ids : continue
    links.append(row)
    if to_id not in to_ids : to_ids.append(to_id)

# Get latest page ranks for strongly connected component
prev_ranks = dict()
for node in from_ids:
    cur.execute('''SELECT new_rank FROM Pages WHERE id = ?''', (node, ))
    row = cur.fetchone()
    prev_ranks[node] = row[0]

iteration_count  = int( input('Count : ') )

for i in range(iteration_count) :
    #find total pagernk
    next_ranks = dict()
    total = 0.0
    for (node,rank) in prev_ranks.items() :
        total = total + rank
        next_ranks[node] = 0.0

    #find the total outbound link for a node
    for (node,rank) in list(prev_ranks.items()):
        give_ids = list()
        for row in links :
            from_id = row[0]
            to_id = row[1]
            if  from_id != node :
                continue
            if  to_id not in to_ids :
                continue
            give_ids.append(to_id)

        if not len(give_ids) < 1 :
            for ids in give_ids :
                next_ranks[ids] = next_ranks[ids] +( rank / len(give_ids) )
            continue


    #find the new total from the next rank dict
    new_total = 0.0
    for (node,rank ) in next_ranks.items() :
        new_total = new_total + rank
    evap = (total - new_total) / len(next_ranks)

    for node in next_ranks :
        next_ranks[node] = next_ranks[node] + evap

    new_total = 0
    for (node,rank) in next_ranks.items() :
        new_total = new_total + rank

    prev_ranks = next_ranks

print(list(next_ranks.items())[:5])
cur.execute('''UPDATE Pages SET old_rank=new_rank''')
for node , rank in next_ranks.items() :
    cur.execute('''UPDATE Pages SET new_rank=? WHERE id=?''', (rank, node))
con.commit()
cur.close()
