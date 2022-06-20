from Config import *
from Utility import Utility

import mysql.connector as mysql

mysql_connection = mysql.connect(**MYSQL_CONFIG_DEVELOP)


def get_id_need_update(**kwargs):
    cursor = mysql_connection.cursor(dictionary=True)
    cursor.execute(f"""
       SELECT id
       FROM nHJKGmsGP4.tasks
       WHERE new_price_apply_date != 0 AND new_price_apply_date < {Utility.current_timestamp()}
   """)
    result = cursor.fetchall()
    cursor.close()
    task_instance = kwargs['task_instance']
    task_instance.xcom_push(
        key='list_id',
        value=[str(item.get("id")) for item in result]
    )


def update_price(list_id):
    if list_id:
        list_id = ",".join(list_id)
        print(f"Update các id: {list_id}")
        cursor = mysql_connection.cursor(dictionary=True)
        cursor.execute(f"""
            UPDATE nHJKGmsGP4.tasks
            SET price = new_selling_price, new_selling_price = 0, new_price_apply_date = 0
            WHERE id IN ({list_id})
        """)
        mysql_connection.commit()
        cursor.close()
    else:
        print("Không update")
    mysql_connection.close()

