from datetime import datetime


def increment_load_controller(dwh_connection):
    load_cursor = dwh_connection.cursor()

    q = 'select day_in_year from dwh.d_date where current_date_ind = 1'
    load_cursor.execute(q)
    load_controller_id = load_cursor.fetchone()[0]

    if load_controller_id != int(datetime.now().strftime('%j')):
        q = 'update dwh.d_date set current_date_ind = 0'
        load_cursor.execute(q)
        dwh_connection.commit()
        day = datetime.now().strftime('%A')
        month = datetime.now().strftime('%B')
        year = datetime.now().year
        day_in_month = datetime.now().day
        month_in_year = datetime.now().month
        quarter = 'Q' + str((int(month_in_year) - 1) // 3 + 1)
        day_in_year = int(datetime.now().strftime('%j'))
        current_date_ind = 1

        q = 'INSERT INTO dwh.d_date(day,month,year,quarter,day_in_year,day_in_month,current_date_ind,month_in_year) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)'
        data = [str(day), str(month), year, str(quarter), day_in_year,
                day_in_month, current_date_ind, month_in_year]

        load_cursor.execute(q, data)
        dwh_connection.commit()


def current_active_id(dwh_connection):
    load_cursor = dwh_connection.cursor()
    q = 'select date_id from dwh.d_date where current_date_ind = 1'
    load_cursor.execute(q)
    cur_date_id = load_cursor.fetchone()[0]
    return cur_date_id
