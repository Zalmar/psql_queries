from psql_q.queries import Queries


def main():
    db = Queries()
    command = f"""CREATE TABLE
    pups(user_id INTEGER PRIMARY KEY, name text, bdate text, city text);"""
    db.create_table(command)

    command = f"""SELECT * FROM pups;"""
    db.select_table(command, limit=10)

    command = f"""INSERT INTO pups(user_id, name, bdate, city) 
    VALUES(1234, 'Pupsik', '10.10.2001', 'Moscow');"""
    db.insert_new_record(command)

    command = f"""INSERT INTO pups(user_id, name, bdate, city) 
    VALUES(123, 'Pupsya', '10.12.2011', 'Samara');"""
    db.insert_new_record(command)

    command = f"""SELECT name,city FROM pups;"""
    db.select_table(command, limit=10)

    command = f"""UPDATE pups SET name = 'Pups' 
    WHERE user_id = 1234;"""
    db.update_record(command)

    command = f"""UPDATE pups SET user_id = 777 
    WHERE name = 'Pups';"""
    db.update_record(command)

    command = f"""SELECT * FROM pups;"""
    db.select_table(command, limit=10)

    db.drop_table('pups')


if __name__ == '__main__':
    main()
