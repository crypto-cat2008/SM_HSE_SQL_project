import psycopg2
import datetime


def start_planning(year, quarter, user, pwd):

    result = None

    target_y_q = str(year) + '.' + str(quarter)
    target_y_1 = str(year - 1) + '.' + str(quarter)
    target_y_2 = str(year - 2) + '.' + str(quarter)

    sql1 = 'delete from plan_data where quarterid = %s'
    sql2 = 'delete from plan_status where quarterid = %s'
    sql3 = 'select distinct countrycode from company'
    sql4 = '''insert into plan_status (quarterid, status, modifieddatetime, author, country)
            values (%s, %s, %s, %s, %s)'''
    sql5 = '''insert into plan_data (versionid, country, quarterid, pcid, salesamt) 
                select 'N', data2.countrycode, %s, data2.pcid, case when sa is null then 0.0 else sa end
                from ( 
                select 'N', c.countrycode, %s, categoryid, sum(salesamt)/2.0 as sa
                from company_sales as cs
                join company as c on c.id =cs.cid 
                where ccls in ('A', 'B') and qr in (%s, %s)
                group by categoryid, c.countrycode ) as data1
                right join 
                (select distinct pcid, countrycode from product2 p 
                cross join country2 c ) as data2 on data2.pcid= data1.categoryid 
                and data2.countrycode=data1.countrycode'''
    sql6 = '''insert into plan_data (versionid, country, quarterid, pcid, salesamt)
                select 'P', country, quarterid, pcid, salesamt from plan_data where versionid = 'N' '''

    con = psycopg2.connect(database='2021_plans_Susan_Malkin', host='localhost', user=user, password=pwd)
    cur = con.cursor()

    try:
        # Step 1 - clean plan_data and plan_status tables
        cur.execute(sql1, [target_y_q])
        cur.execute(sql2, [target_y_q])

        # Step 2 - create plan_status records
        cur.execute(sql3)
        countries = [c[0] for c in cur]
        for c in countries:
            curr_time = datetime.datetime.now()
            cur.execute(sql4, [target_y_q, 'R', curr_time, user, c])

        # Step 3, 4 - create plan_data records
        cur.execute(sql5, [target_y_q, target_y_q, target_y_1, target_y_2])
        cur.execute(sql6)
        result = 1

        con.commit()

    except Exception as err:
        print(err)

    con.close()

    return result


def set_lock(year, quarter, user, pwd):

    result = None
    target_y_q = str(year) + '.' + str(quarter)
    test = True

    sql1 = '''select current_user'''
    sql2 = '''select * from country_managers where username = %s'''
    sql3 = '''update plan_status 
            set status = 'L', modifieddatetime = %s, author = %s
            where quarterid = %s and country = %s'''
    sql4 = '''select * from v_plan_edit'''

    con = psycopg2.connect(database='2021_plans_Susan_Malkin', host='localhost', user=user, password=pwd)
    cur = con.cursor()

    try:
        cur.execute(sql1)
        cur_user = cur.fetchone()[0]   # get current user

        cur.execute(sql2, [cur_user])   # get countries for current user
        countries = [c[1] for c in cur]

        for c in countries:
            curr_time = datetime.datetime.now()
            cur.execute(sql3, [curr_time, cur_user, target_y_q, c])

        if test:
            cur.execute(sql4)
            for c in cur:
                print(c)

        con.commit()
        result = 1

    except Exception as err:
        print(err)

    con.close()

    return result


def remove_lock(year, quarter, user, pwd):
    result = None
    target_y_q = str(year) + '.' + str(quarter)

    sql1 = '''select current_user'''
    sql2 = '''select * from country_managers where username = %s'''
    sql3 = '''update plan_status 
               set status = 'R', modifieddatetime = %s, author = %s
               where quarterid = %s and country = %s'''

    con = psycopg2.connect(database='2021_plans_Susan_Malkin', host='localhost', user=user, password=pwd)
    cur = con.cursor()

    try:
        cur.execute(sql1)
        cur_user = cur.fetchone()[0]   # get current user

        cur.execute(sql2, [cur_user])   # get countries for current user
        countries = [c[1] for c in cur]

        for c in countries:
            curr_time = datetime.datetime.now()
            cur.execute(sql3, [curr_time, cur_user, target_y_q, c])

        con.commit()
        result = 1

    except Exception as err:
        print(err)

    con.close()

    return result


def accept_plan(year, quarter, user, pwd):
    result = None

    target_y_q = str(year) + '.' + str(quarter)
    sql1 = '''select current_user'''
    sql2 = '''select * from country_managers where username = %s'''
    sql3 = '''update plan_status 
                set status = %s, modifieddatetime = %s, author = %s
                where quarterid = %s and country = %s'''
    sql4 = '''delete from plan_data pd 
                where versionid = 'A' and pd.quarterid = %s and 
                (select status from plan_status where country=%s and quarterid = %s) = 'R' and 
                pd.country =%s'''
    sql5 = '''insert into plan_data (versionid, country, quarterid, pcid, salesamt)
                select 'A', country, quarterid, pcid, salesamt 
                from plan_data where versionid = 'P' and country = %s and quarterid = %s
                and (select status from plan_status where country=%s and quarterid = %s) = 'R' '''

    con = psycopg2.connect(database='2021_plans_Susan_Malkin', host='localhost', user=user, password=pwd)
    cur = con.cursor()

    try:
        cur.execute(sql1)
        cur_user = cur.fetchone()[0]  # get current user

        # clean approved version of the plan
        cur.execute(sql2, [cur_user])  # get countries for current user
        countries = [c[1] for c in cur]

        for c in countries:
            curr_time = datetime.datetime.now()
            cur.execute(sql3, ['R', curr_time, cur_user, target_y_q, c])     # reset status from A to R
            cur.execute(sql4, [target_y_q, c, target_y_q, c])               # delete A records
            cur.execute(sql5, [c, target_y_q, c, target_y_q])               # create approved version of the plan
            cur.execute(sql3, ['A', curr_time, cur_user, target_y_q, c])    # reset status from R to A

            print(f'user {cur_user} country {c}')

        con.commit()
        result = 1

    except Exception as err:
        print(err)

    con.close()

    return result



users = ['sophie', 'kirill']
pwds = ['sql2', 'sql3']

for i in range(len(users)):
    if accept_plan(2014, 1, users[i], pwds[i]):
        print(f'Successful plan accept! >>> {users[i]}')


