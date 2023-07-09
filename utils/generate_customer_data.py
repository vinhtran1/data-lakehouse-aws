

import random
import json
import string
import uuid
import datetime
from itertools import groupby
import os

def get_random_string(length):
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    result_str = result_str.capitalize()
    return result_str

def get_payload(record):
    payload = record.get("payload")
    if record.get("type") == "Update":
        payload = record.get("payload").get("after")
    return payload

def get_id(record):
    if record.get("type") == "Insert":
        id = record.get("payload").get("id")
    elif record.get("type") == "Update":
        id = record.get("payload").get("after").get("id")
    return id

def get_updated_date(record):
    if record.get("type") == "Insert":
        id = record.get("payload").get("updated_date")
    elif record.get("type") == "Update":
        id = record.get("payload").get("after").get("updated_date")
    return id

def find_all_records_by_id(records, id_to_match):
    records_found = []
    for record in records:
        id = get_id(record)
        if id == id_to_match:
            records_found.append(record)
    return records_found

def has_id_deleted(records, id_to_match):
    for record in records:
        if record.get("type") == "Delete":
            id = record.get("payload").get("id")
            if id == id_to_match:
                return True
    return False

def insert_new_customer():
    startdate=datetime.datetime(2020,1,1)
    date=startdate+datetime.timedelta(random.randint(1,10)) + datetime.timedelta(seconds=random.randint(1,86399))
    date_str = date.isoformat()
    payload = {"id": str(uuid.uuid4()), 
                "name": get_random_string(5), 
                "age": random.randint(18, 65), 
                "created_date": date_str, 
                "updated_date":date_str,
                "deleted_date":None,
                }
    record = {"table": "customer", 
                "type":"Insert", 
                "payload": payload
    }
    return record

def update_customer(customers:list):
    while True:
        record_old = random.choice(customers)
        if record_old.get("type") != "Delete":
            break
    payload_old = get_payload(record_old)
    tmp1 = datetime.datetime.fromisoformat(payload_old.get("updated_date"))
    delta1 = datetime.timedelta(random.randint(1,10))
    delta2 = datetime.timedelta(seconds=random.randint(1,86399))
    updated_date = tmp1 + delta1 + delta2
    updated_date_str = updated_date.isoformat()
    payload_new = {"id": record_old.get("id"), 
                "name": get_random_string(5), 
                "age": random.randint(18, 65), 
                "created_date": payload_old.get("updated_date"), 
                "updated_date":updated_date_str,
                "deleted_date": None
                }
    record = {
        "table": "customer", 
        "type":"Update",
        "payload": {
            "before": payload_old,
            "after": payload_new
        }
    }
    return record

def delete_customer(customers):
    while True:
        record_old = random.choice(customers)
        payload_id = get_id(record_old)
        updated_date = get_updated_date(record_old)
        if record_old.get("type") != "Delete" and has_id_deleted(customers, payload_id) is False:
            break
    deleted_date = datetime.datetime.fromisoformat(updated_date) + datetime.timedelta(random.randint(1,10)) + datetime.timedelta(seconds=random.randint(1,86399))
    deleted_date_str = deleted_date.isoformat()
    payload = {"id": payload_id , 
                "updated_date": deleted_date_str,
                "deleted_date":deleted_date_str
    }
    record = {
        "table": "customer", 
        "type":"Delete",
        "payload": payload
    }
    return record

def generate_list_random_customer_data(num_records=1):
    lst_records = []
    for i in range(1, num_records+1):
        if i % 10 == 0 and i % 100 != 0:
            data = update_customer(lst_records)
            lst_records.append(data)
        elif i % 100 == 0:
            data = delete_customer(lst_records)
            lst_records.append(data)
        else:
            data = insert_new_customer()
            lst_records.append(data)
    return lst_records

def group_by_date(records):
    lst_data_grouped_by_date = []
    def key_func(k):
        try:
            return k['payload']['updated_date'][:10]
        except KeyError:
            return k['payload']['after']['updated_date'][:10]
    lst_records = sorted(records, key=key_func)
    # return lst_records
    for key, value in groupby(lst_records, key_func):
        tmp_dict = {
            "date": key,
            "value": list(value)
        }
        lst_data_grouped_by_date.append(tmp_dict)
    return lst_data_grouped_by_date

def sort_by_date(records):
    def key_func(k):
        try:
            return k['payload']['updated_date'][:10]
        except KeyError:
            return k['payload']['after']['updated_date'][:10]
    records_sorted = sorted(records, key=key_func)
    return records_sorted

        
def export_to_jsons(dict_records):
    json_string = json.dumps(dict_records)
    for key, value in dict_records.items():
        with open(os.path.join('.','data','customer', key + '.json'), 'w') as f:
            f.write(json.dumps(value))
    with open(os.path.join('.','data','customer', 'total.json'), 'w') as f:
        f.write(json_string)

def generate_stream_customer_data(num_records=1):
    lst_data = generate_list_random_customer_data(num_records)
    lst_data_sorted = sort_by_date(lst_data)
    return lst_data_sorted


def generate_batch_customer_data(num_records=1):
    lst_data = generate_list_random_customer_data(num_records)
    grouped_data = group_by_date(lst_data)
    return grouped_data


def genrate_transaction_data(customer_ids, num_transaction=1, company_name= 'companyA'):
    lst_transactions = []
    startdate=datetime.datetime(2020,1,1)
    for _ in range(num_transaction):
        customer_id = random.choice(customer_ids)
        order_id = str(uuid.uuid4())
        if company_name == 'companyA':
            order_value = random.randint(10**5, 5*10**7) # 100K to 50M
        elif company_name == 'companyB':
            order_value = random.randint(10**9, 10**10) # 1 billion to 10 billion
        order_date=startdate+datetime.timedelta(random.randint(1,10)) + datetime.timedelta(seconds=random.randint(1,86399))
        order_date_str = order_date.isoformat()
        currency = random.choice(['USD', 'VND'])
        if currency == 'USD':
            order_value = order_value/23500.0
        transaction_tmp = {
            "customer_id": customer_id,
            "order_date":order_date_str,
            "order_value": order_value,
            "order_id": order_id,
            "currency": currency
        }
        lst_transactions.append(transaction_tmp)
    return lst_transactions


    