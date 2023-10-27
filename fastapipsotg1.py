from neo4j import GraphDatabase
import time
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import threading
from psycopg2 import connect, sql
from pydantic import BaseModel

app = FastAPI()
event_listener = None

class EventListener:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def poll_for_new_persons(self):
        while True:
            with self._driver.session() as session:
                try:
                    result = session.run("MATCH (p:Person) WHERE NOT p.processed RETURN p.name AS name LIMIT 1")
                    new_person_found = False
                    for record in result:
                        person_name = record["name"]
                        self.on_person_created(person_name)
                        session.run("MATCH (p:Person {name: $name}) SET p.processed = true", name=person_name)
                        new_person_found = True
                    if not new_person_found:
                        print("No new unprocessed persons found.")
                except Exception as e:
                    print(f"An error occurred: {str(e)}")
                time.sleep(10)

    def on_person_created(self, person_name):
        print(f"New person created: {person_name}")
        person_detail = get_person_detail(person_name)
        insert_person_detail(person_name, person_detail)

@app.exception_handler(HTTPException)
async def custom_http_exception_handler(request, exc):
    return JSONResponse(status_code=exc.status_code, content={"detail": "Not Found"})

class UpdatePersonModel(BaseModel):
    age: int
    city: str

@app.put("/person/{name}")
def update_person_detail(name: str, update_data: UpdatePersonModel):
    with GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password)) as driver:
        with driver.session() as session:
            result = session.run("MATCH (p:Person {name: $name}) RETURN p.name AS name", name=name)
            record = result.single()
            if record is None:
                raise HTTPException(status_code=404, detail="Person not found")
            session.run("MATCH (p:Person {name: $name}) SET p.age = $age, p.city = $city", name=name, age=update_data.age, city=update_data.city)

    update_person_detail(name, update_data)

    return {"message": "Person details updated successfully"}

@app.get("/person/{name}")
def get_person_detail(name: str):
    with GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password)) as driver:
        with driver.session() as session:
            result = session.run("MATCH (p:Person {name: $name}) RETURN p.name AS name, p.age AS age, p.city AS city", name=name)
            record = result.single()
            if record is not None:
                person_detail = {
                    "name": record["name"],
                    "age": record["age"],
                    "city": record["city"]
                }
                return person_detail

    raise HTTPException(status_code=404, detail="Person not found")

def insert_person_detail(name, detail):
    try:
        connection = connect(
            host=postgres_host,
            database=postgres_db,
            user=postgres_user,
            password=postgres_password
        )
        cursor = connection.cursor()
        query = sql.SQL("INSERT INTO api_endpoint_details (endpoint_name, detail_text) VALUES ({}, {});").format(
            sql.Literal(f"Person Details: {name}"),
            sql.Literal(f"Name: {name}, Age: {detail['age']}, City: {detail['city']}")
        )
        cursor.execute(query)
        connection.commit()
        cursor.close()
        connection.close()
    except Exception as e:
        print(f"An error occurred while inserting data into the database: {str(e)}")

neo4j_uri = "neo4j+s://934a88d6.databases.neo4j.io"
neo4j_user = "neo4j"
neo4j_password = "DPSAB-Ar4ELk-S1tit4UARxoUUEiTYNqBn-QaO7kwQM"

if event_listener is None:
    event_listener = EventListener(neo4j_uri, neo4j_user, neo4j_password)

postgres_host = "suleiman.db.elephantsql.com"
postgres_db = "svcffrcx"
postgres_user = "svcffrcx"
postgres_password = "UqzcWSOIkEg7ZCqf9WdRHof-elumRY-e"

if __name__ == "__main__":
    listener_thread = threading.Thread(target=event_listener.poll_for_new_persons)
    listener_thread.daemon = True
    listener_thread.start()
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
