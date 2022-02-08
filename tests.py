# if start_planning(2014, 1, 'ivan', 'sql1'):
#    print('Success! >>> start_planning')


users = ['sophie', 'kirill']
pwds = ['sql2', 'sql3']

for i in range(len(users)):
    #if set_lock(2014, 1, users[i], pwds[i]):
        #print(f'Successful lock set! >>> {users[i]}')

    if remove_lock(2014, 1, users[i], pwds[i]):
        print(f'Successful lock remove! >>> {users[i]}')