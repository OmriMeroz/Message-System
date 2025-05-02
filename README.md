# Message System Project

## Overview
This project is a messaging system built using Flask, Kafka, and S3. It allows users to send messages which are stored in an S3 bucket and are processed via Kafka. Users are authenticated via JWT, and their credentials are managed securely. The project also includes a simple registration and login system, which you can interact with via Postman or Swagger.

## Features
- **User Authentication** using JWT (JSON Web Tokens)
- **Message Sending**: Users can send messages which are processed and stored in an S3 bucket via Kafka.
- **Consumer Service**: Messages are consumed from Kafka and saved to the S3 bucket.
- **Environment Variables**: All sensitive data (API keys, credentials) are stored in a `.env` file.

## Prerequisites
To run this project, you need to have the following installed:
- Python 3.8+
- Kafka (either local or remote)
- AWS account for S3
- SQLite
- Docker (for containerization, optional)
- Postman (for testing, optional)

## Installation

### 1. Clone the repository
First, clone the repository to your local machine:

```bash
git clone https://github.com/yourusername/message-system.git
cd message-system

2. Install dependencies
pip install -r requirements.txt

3. Setup environment variables
FLASK_APP=app.py
FLASK_ENV=development
KAFKA_BROKER_URL=your-kafka-broker-url
AWS_ACCESS_KEY_ID=your-aws-access-key-id
AWS_SECRET_ACCESS_KEY=your-aws-secret-access-key
AWS_REGION=your-aws-region
AWS_S3_BUCKET_NAME=your-s3-bucket-name
SECRET_KEY=your-secret-key
JWT_SECRET_KEY=your-jwt-secret-key

4. Run the Flask application
flask run


**Usage**
1. User Registration

To register a new user, send a POST request to /register with the following JSON body:
{
  "username": "user1",
  "password": "yourpassword"
}

2. User Login
To login, send a POST request to /login with the following JSON body:
{
  "username": "user1",
  "password": "yourpassword"
}

3. Send Message
Once you are logged in, you can send a message by sending a POST request to /message with the following JSON body:
{
  "message": "Hello, this is a test message!"
}
The message will be sent to Kafka, and from there, it will be saved to your S3 bucket.

4. View Messages
To view messages, you can send a GET request to /messages. This will return all the messages that have been successfully stored in S3.


Testing
You can use Postman or Swagger to test the endpoints. Swagger is available at http://127.0.0.1:5000/swagger.

Docker (Optional)
If you want to run the application inside a Docker container, you can build and run the container with the following commands:

docker build -t message-system .
docker run -p 5000:5000 --env-file .env message-system

This will start the application inside a container and expose it on port 5000.




