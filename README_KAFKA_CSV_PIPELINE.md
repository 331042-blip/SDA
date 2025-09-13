Kafka CSV Pipeline - Files & Setup
---------------------------------

Files included:
- producer_csv.py         -> reads employees.csv and streams rows to Kafka (employee-topic)
- consumer.py             -> simple consumer that prints messages from employee-topic
- consumer_to_mongo.py    -> consumes from Kafka and inserts messages into MongoDB
- producer_to_mongo.py    -> reads CSV, publishes to Kafka AND inserts into MongoDB (optional)
- docker-compose.yml      -> starts Zookeeper, Kafka, and MongoDB containers
- requirements.txt        -> python dependencies (kafka-python, pymongo)
- employees.csv           -> sample CSV (you should replace with your actual data)

Step-by-step setup (Linux / macOS / Windows WSL recommended)
-----------------------------------------------------------

1) Install prerequisites
   - Docker & Docker Compose (Docker Desktop on Windows/macOS).
   - Python 3.8+ and pip.

2) Start the services with Docker Compose
   In the directory containing docker-compose.yml, run:
     docker-compose up -d

   Confirm containers are up:
     docker ps

   Kafka should be reachable at localhost:9092 and MongoDB at localhost:27017.

   (If you have issues connecting to Kafka from host, see Troubleshooting below.)

3) (Optional) Create the Kafka topic manually (if auto-create is disabled)
     docker-compose exec kafka kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic employee-topic

4) Create & activate a Python virtual environment
     python3 -m venv venv
     source venv/bin/activate        # macOS / Linux
     venv\Scripts\activate.bat       # Windows (cmd) or use PowerShell variant

5) Install Python dependencies
     pip install --upgrade pip
     pip install -r requirements.txt

6) Place your CSV file
   - Replace employees.csv with your actual file (same folder as the scripts).
   - The scripts try to auto-detect delimiter (comma or tab). Ensure header row matches the columns:
     Employee_ID,Full_Name,Department,Job_Title,Hire_Date,Location,Performance_Rating,Experience_Years,Status,Work_Mode,Salary_INR

7) Run the producer (streams CSV periodically)
     python producer_csv.py

8) In another terminal run the consumer to verify messages
     python consumer.py

9) To persist messages into MongoDB (option A: consumer writes to MongoDB)
     python consumer_to_mongo.py

   (Option B: producer_to_mongo.py will both publish and store — use instead of the plain producer if desired.)

10) Verify data in MongoDB
    - Use MongoDB Compass or the mongo shell:
      mongo --host localhost --port 27017
      > use employee_db
      > db.employees.find().pretty()

Stopping everything
-------------------
- Stop Python scripts with CTRL+C.
- Stop and remove containers:
    docker-compose down

Troubleshooting
---------------
- "Kafka broker not available": ensure Docker containers are running and that the Kafka container advertises listeners reachable from host.
- If on Mac/Windows and Kafka isn't reachable via localhost:9092, try replacing KAFKA_ADVERTISED_LISTENERS with PLAINTEXT://host.docker.internal:9092 in docker-compose.yml (and recreate containers).
- If topic auto-creation is disabled, create it using the kafka-topics command shown above.
- Check container logs:
    docker-compose logs kafka
    docker-compose logs zookeeper
    docker-compose logs mongo

That's it — you can now stream your employee CSV records through Kafka periodically and optionally persist them in MongoDB.