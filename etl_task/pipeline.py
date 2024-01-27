
import json
import random
import string
from datetime import datetime, timedelta


def generate_random_number_string(length=18):
    random_numbers = ''.join(random.choices(string.digits, k=length))
    return random_numbers


def load_user_temp(connection, active_load_id):

    select_load_id = "select max(load_id) from stage.user_temp"
    cursor = connection.cursor()
    cursor.execute(select_load_id)

    load_id = cursor.fetchone()[0]

    if str(load_id) != str(active_load_id):

        with open('data/person.json', 'r') as p:
            person = json.load(p)

        with open('data/party.json', 'r') as pa:
            party = json.load(pa)

        for key, value in person.items():

            data = [value['first_name'], value['last_name'],
                    value['unique_identification_number'], value['phone_number'], active_load_id]

            query = "select count(*) from stage.user_temp where unique_identification_number = %s"
            cursor.execute(
                query, [value['unique_identification_number']])

            duplicates = cursor.fetchone()[0]

            if duplicates == 0:

                insert_into_user_temp_q = "INSERT INTO stage.user_temp(first_name, last_name, unique_identification_number, number, load_id) VALUES(%s, %s, %s, %s, %s)"
                cursor.execute(insert_into_user_temp_q, data)
                connection.commit()

        for key, value in party.items():

            data = [value['party_name'], value['pib'], active_load_id]

            query = "select count(*) from stage.user_temp where pib = %s"
            cursor.execute(
                query, [value['pib']])

            duplicates = cursor.fetchone()[0]

            if duplicates == 0:

                insert_into_user_temp_q = "INSERT INTO stage.user_temp(company_name, pib, load_id) VALUES(%s, %s, %s)"
                cursor.execute(insert_into_user_temp_q, data)
                connection.commit()


def load_location_temp(connection, active_load_id):

    select_load_id = "select max(load_id) from stage.location_temp"
    cursor = connection.cursor()
    cursor.execute(select_load_id)

    load_id = cursor.fetchone()[0]

    if str(load_id) != str(active_load_id):
        with open('data/supermarkets.json', 'r') as s:
            supermarkets = json.load(s)

        with open('data/clothing_stores.json', 'r') as c:
            clothing_stores = json.load(c)

        for key, value in supermarkets.items():

            data = [active_load_id, value['name'], 'S', key, value['address']]

            query = "select count(*) from stage.location_temp where source_id = %s and location_source_category = 'S'"
            cursor.execute(
                query, [key])

            duplicates = cursor.fetchone()[0]

            if duplicates == 0:
                insert_into_location_temp = "INSERT INTO stage.location_temp(load_id,location_name,location_source_category,source_id, address) VALUES (%s,%s,%s,%s,%s)"
                cursor.execute(insert_into_location_temp, data)
                connection.commit()

        for key, value in clothing_stores.items():

            data = [active_load_id, value['name'], 'C', key]

            query = "select count(*) from stage.location_temp where source_id = %s and location_source_category = 'C'"
            cursor.execute(
                query, [key])

            duplicates = cursor.fetchone()[0]

            if duplicates == 0:
                insert_into_location_temp = "INSERT INTO stage.location_temp(load_id,location_name,location_source_category,source_id) VALUES (%s,%s,%s,%s)"
                cursor.execute(insert_into_location_temp, data)
                connection.commit()


def load_d_location(dwh_connection, stage_connection, active_load_id):

    dwh_cursor = dwh_connection.cursor()

    select_load_id = "select max(load_id) from dwh.d_location"
    dwh_cursor.execute(select_load_id)
    load_id = dwh_cursor.fetchone()[0]

    stage_cursor = stage_connection.cursor()

    if str(load_id) != str(active_load_id):

        query = "SELECT location_id, location_name, location_source_category, source_id, address, load_id FROM stage.location_temp"
        stage_cursor.execute(query)
        loc_res = stage_cursor.fetchall()
        for data in loc_res:

            q = "select count(*) from dwh.d_location where location_id = %s"
            dwh_cursor.execute(q, [data[0]])

            duplicates = dwh_cursor.fetchone()[0]

            if duplicates > 0:
                continue

            list(data).append(active_load_id)
            insert_into_d_location_q = "INSERT INTO dwh.d_location(location_id, location_name, location_source_category, source_id, address, load_id) values(%s,%s,%s,%s,%s,%s)"
            dwh_cursor.execute(insert_into_d_location_q, data)
            dwh_connection.commit()


def load_d_user(dwh_connection, stage_connection, active_load_id):
    select_load_id = "select max(load_id) from dwh.d_user"
    stage_cursor = stage_connection.cursor()
    stage_cursor.execute(select_load_id)

    dwh_cursor = dwh_connection.cursor()

    load_id = stage_cursor.fetchone()[0]

    if str(load_id) != str(active_load_id):

        select_all_user_tmp = "SELECT company_name,first_name,unique_identification_number,last_name,latency_period,number,pib,risk_group,user_id, load_id FROM stage.user_temp"

        stage_cursor.execute(select_all_user_tmp)
        user_res = stage_cursor.fetchall()

        for data in user_res:
            q = 'select count(*) from dwh.d_user where user_id = %s'
            dwh_cursor.execute(q, [data[8]])
            duplicates = dwh_cursor.fetchone()[0]

            if duplicates > 0:
                continue

            list(data).append(active_load_id)
            insert_into_d_user_q = "INSERT INTO dwh.d_user(company_name,first_name,unique_identification_number,last_name,latency_period,number,pib,risk_group,user_id, load_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            dwh_cursor.execute(insert_into_d_user_q, data)
            dwh_connection.commit()


def load_d_account(stage_connection, dwh_connection, active_load_id):
    dwh_cursor = dwh_connection.cursor()
    stage_cursor = stage_connection.cursor()

    select_load_id = "select max(load_id) from dwh.d_account"
    dwh_cursor.execute(select_load_id)
    load_id = dwh_cursor.fetchone()[0]

    if str(load_id) != str(active_load_id):

        q_user_ids = "SELECT user_id FROM dwh.d_user"
        dwh_cursor.execute(q_user_ids)
        user_ids = dwh_cursor.fetchall()

        for user_id in user_ids:

            q = "select count(*) from dwh.d_account where placeholder_id = %s"
            dwh_cursor.execute(q, [user_id[0]])

            duplicates = dwh_cursor.fetchone()[0]

            if duplicates > 0:
                continue

            up_acc_seq = "UPDATE dwh.account_sequence SET id= (select LAST_INSERT_ID(id+1))"
            dwh_cursor.execute(up_acc_seq)
            dwh_connection.commit()

            q = "SELECT id FROM dwh.account_sequence"
            dwh_cursor.execute(q)
            acc_seq = dwh_cursor.fetchone()

            new_credit_card_num = generate_random_number_string(20)

            cc_sql = 'select credit_card_number from dwh.d_account where credit_card_number = %s'
            dwh_cursor.execute(cc_sql, [new_credit_card_num])
            existing_credit_card = dwh_cursor.fetchall()

            while len(existing_credit_card) > 0:

                new_credit_card_num = generate_random_number_string(20)
                cc_sql = 'select credit_card_number from dwh.d_account where credit_card_number = %s'
                dwh_cursor.execute(cc_sql, [new_credit_card_num])
                existing_credit_card = dwh_cursor.fetchall()

            insert_into_d_acc_q = "INSERT INTO dwh.d_account(account_id,account_number,credit_card_number,membership,placeholder_id, load_id, balance, currency) VALUES(%s,%s,%s,%s,%s, %s, %s, %s)"
            data = [acc_seq[0], generate_random_number_string(),
                    new_credit_card_num, 'standard', user_id[0], active_load_id, 1000, 'EUR']

            dwh_cursor.execute(insert_into_d_acc_q, data)
            dwh_connection.commit()


def increment_date(connection):
    query_current_state = 'select day_in_month, month_in_year, year, date_id from dwh.d_date where current_date_ind = 1'

    cursor = connection.cursor()
    cursor.execute(query_current_state)
    current_state = cursor.fetchall()

    date_id = current_state[0][3]

    c_date = datetime.strptime(
        str(current_state[0][2]) + '-' + str(current_state[0][1]) + '-' + str(current_state[0][0]), '%Y-%m-%d')

    if len(current_state) == 0 or c_date.strftime('%Y-%m-%d') != datetime.today().strftime('%Y-%m-%d'):

        new_date = c_date + timedelta(1)
        day = new_date.strftime('%A')
        month = new_date.strftime('%B')
        year = new_date.year
        day_in_month = new_date.day
        month_in_year = new_date.month
        quarter = 'Q' + str((int(month_in_year) - 1) // 3 + 1)
        day_in_year = int(new_date.strftime('%j'))
        current_date_ind = 1

        data = [day, month, year, quarter, day_in_month,
                month_in_year, day_in_year, current_date_ind]

        update_query = 'update dwh.d_date set current_date_ind = 0 where date_id = %s'
        insert_query = 'insert into dwh.d_date(day,month,year,quarter,day_in_month,month_in_year,day_in_year, current_date_ind) values(%s,%s,%s,%s,%s,%s,%s,%s)'

        cursor.execute(update_query, [date_id])
        cursor.execute(insert_query, data)
        connection.commit()


def load_d_date(connection, dt):
    cursor = connection.cursor()

    day_in_year = int(dt.strftime('%j'))

    q = 'select case when %s in (select day_in_year from dwh.d_date) then 1 else 0 end as result'
    cursor.execute(q, [day_in_year])

    q = cursor.fetchone()

    if q[0] == 0:
        day = dt.strftime('%A')
        month = dt.strftime('%B')
        year = dt.year
        day_in_month = dt.day
        month_in_year = dt.month
        quarter = 'Q' + str((int(month_in_year) - 1) // 3 + 1)
        day_in_year = int(dt.strftime('%j'))
        current_date_ind = 0

        data = [day, month, year, quarter, day_in_month,
                month_in_year, day_in_year, current_date_ind]

        insert_query = 'insert into dwh.d_date(day,month,year,quarter,day_in_month,month_in_year,day_in_year, current_date_ind) values(%s,%s,%s,%s,%s,%s,%s,%s)'
        cursor.execute(insert_query, data)
        connection.commit()


def load_f_transactions(dwh_connection, stage_connection, active_load_id):
    stage_cursor = stage_connection.cursor()
    dwh_cursor = dwh_connection.cursor()

    q = 'select day_in_year, year from dwh.d_date where current_date_ind = 1'
    dwh_cursor.execute(q)
    day_in_year, year = dwh_cursor.fetchone()

    date = datetime(year, 1, 1) + timedelta(days=day_in_year-1)

    q = "select * from stage.purchases where date_format(purchase_date, '%Y-%m-%d') <= %s"

    stage_cursor.execute(q, [str(date.strftime('%Y-%m-%d'))])
    data = stage_cursor.fetchall()

    for record in data:

        p_id = record[0]

        q = 'select count(*) from dwh.f_transactions where purchase_id = %s'
        dwh_cursor.execute(q, [p_id])

        purchase_id = dwh_cursor.fetchone()[0]

        if purchase_id > 0:
            continue

        location_q = 'select location_id from dwh.d_location where location_name = %s and address = %s and location_source_category = %s'
        d = [record[2], record[3], record[10]]

        dwh_cursor.execute(location_q, [d])

        location_id = dwh_cursor.fetchone()[0]
        amount = record[1]
        transaction_type = record[4]

        acc_q = 'select account_id, placeholder_id from dwh.d_account where credit_card_number = %s'
        d = [record[5]]
        dwh_cursor.execute(acc_q, [d])

        account_id, user_id = dwh_cursor.fetchone()

        load_d_date(dwh_connection, record[8])

        query = 'select date_id from dwh.d_date where day_in_year = %s'
        data = int(record[8].strftime('%j'))
        dwh_cursor.execute(query, [data])

        trnasaction_date_id = dwh_cursor.fetchone()[0]

        currency = record[6]

        is_successful = record[7]

        insert_query_f = 'insert into dwh.f_transactions(location_id, account_id, transaction_date_id, user_id, currency, is_successful, amount, transaction_type, purchase_id) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        data = [location_id, account_id, trnasaction_date_id, user_id,
                currency, is_successful, amount, transaction_type, p_id]

        dwh_cursor.execute(insert_query_f, data)
        dwh_connection.commit()
