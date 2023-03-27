from pymongo import MongoClient
from datetime import datetime as dt
import pprint
from bson.objectid import ObjectId

connection_string = "mongodb+srv://emre:1234@test1.6bsz8bk.mongodb.net/?retryWrites=true&w=majority&authSource=admin"
client = MongoClient(connection_string)
db = client.comp_db
employers = db.employers

printer = pprint.PrettyPrinter()

def employers_validator():
    validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["first_name", "last_name", "date_of_birth", "mail_address", "phone_number", "date_of_employed"],
            "properties": {
                "first_name": {
                    "bsonType": "string",
                    "description": "must be a string and is required"
                },
                "last_name": {
                    "bsonType": "string",
                    "description": "must be a string and is required"
                },
                "date_of_birth": {
                    "bsonType": "date",
                    "description": "must be a date and is required"
                },
                "mail_address": {
                    "bsonType": "string",
                    "pattern": "(\S)+@(\S)+(\.)(\S)+",
                    "description": "must be in e-mail address format and is required"
                },
                "phone_number": {
                    "bsonType": "string",
                    "pattern": "(\(\+(\d)+\))?(\s)?(\d\d\d)(\s)?(\d\d\d)(\s)?(\d\d)(\s)?(\d\d)(\s)?",
                    "description": "must be in phone number format and is required"
                },
                "date_of_employed": {
                    "bsonType": "date",
                    "description": "must be a date and is required"
                }
            }
        }
    }

    db.command("collMod", "employers", validator=validator)

def insert_test_data():
    
    test_data = {
        "first_name": "Emre",
        "last_name": "Aydogmus",
        "date_of_birth": dt(2004, 11, 8),
        "mail_address": "emreayd1923@gmail.com",
        "phone_number": "(+90) 541 960 6432",
        "date_of_employed": dt(2023, 3, 27)
    }
    inserted_id = employers.insert_one(test_data).inserted_id
    print(inserted_id)

def insert_new_data():
    global employers

    f_name = input("First Name> ")
    l_name = input("last Name> ")
    
    db_input = input("Date of Birth> ")
    db_lst = db_input.split(".")
    db = dt(int(db_lst[2]), int(db_lst[1]), int(db_lst[0]),)

    mail = input("E-mail Adress> ")
    num = input("Phone Number> ")

    de_input = input("Date of Employment> ")
    de_lst = de_input.split(".")
    de = dt(int(de_lst[2]), int(de_lst[1]), int(de_lst[0]),)

    new_data = {
        "first_name": f_name,
        "last_name": l_name,
        "date_of_birth": db,
        "mail_address": mail,
        "phone_number": num,
        "date_of_employed": de
    }

    inserted_id = employers.insert_one(new_data).inserted_id
    _id = ObjectId(inserted_id)
    employer = employers.find_one({"_id": _id})
    printer.pprint(employer)
    print("Saved Successfully")



def show_data():
    global employers
    employers = employers.find()

    for employer in employers:
        printer.pprint(employer)


def search_employer():
    global employers
    search = input("Search>")
    field = input("""Searching Field 
    (first_name, last_name, mail_address, phone_number)
    > """)
    result = employers.aggregate([
        {
            "$search": {
                "index": "employer_search",
                "text": {
                    "query": search,
                    "path": field
                }
            }
        }
    ])
    print(list(result))

def search_by_date():
    global employers

    date_type = input("""Type of Date
    (date_of_birth, date_of_employed)
    >""")


    min_date_input = input("Starting Date> ")
    min_date_lst = min_date_input.split(".")
    min_date = dt(int(min_date_lst[2]), int(min_date_lst[1]), int(min_date_lst[0]),)

    max_date_input = input("Ending Date> ")
    max_date_lst = max_date_input.split(".")
    max_date = dt(int(max_date_lst[2]), int(max_date_lst[1]), int(max_date_lst[0]),)

    query = {date_type:{"$gte": min_date,"$lte": max_date}}

    employers = employers.find(query)
    for employer in employers:
        printer.pprint(employer)

def delete_data_by_id():
    global employers
    person_id = input("Id of Data> ")
    _id = ObjectId(person_id)
    employers.delete_one({"_id": _id})
    print("Deleted Successfully")

if __name__ == "__main__":
    
    command = ""
    print("Commands: 'exit', 'show datas', 'save new data', 'search data', 'search by date', 'delete data'.")
    
    while True:
        command = input("Command>")
        if command.lower() == "exit":
            break
        elif command.lower() == "save new data":
            insert_new_data()
        elif command.lower() == "show datas":
            show_data()
        elif command.lower() == "search data":
            search_employer()
        elif command.lower() == "search by date":
            search_by_date()
        elif command.lower() == "delete data":
            delete_data_by_id()




