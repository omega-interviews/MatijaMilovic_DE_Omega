from load_controller import current_active_id, increment_load_controller
from db_connection import close_connecttion, open_connection
from analyse_data import location_chart, date_chart

from pipeline import *


if __name__ == '__main__':

    stage_connection, dwh_connection = open_connection()

    increment_load_controller(dwh_connection)
    active_load_id = current_active_id(dwh_connection)

    load_user_temp(stage_connection, active_load_id)
    load_location_temp(stage_connection, active_load_id)
    load_d_user(dwh_connection, stage_connection, active_load_id)
    load_d_account(dwh_connection, stage_connection, active_load_id)
    load_d_location(dwh_connection, stage_connection, active_load_id)
    increment_date(dwh_connection)
    load_f_transactions(dwh_connection, stage_connection, active_load_id)

    location_chart(dwh_connection)
    date_chart(dwh_connection)
    close_connecttion(stage_connection)
    close_connecttion(dwh_connection)
