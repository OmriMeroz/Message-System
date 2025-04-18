import boto3
from flask import Flask, request, jsonify
from flasgger import Swagger
from kafka import KafkaProducer
import json
import os
from datetime import datetime
from flask_jwt_extended import JWTManager, create_access_token
from datetime import timedelta
from flask_jwt_extended import create_access_token, create_refresh_token
from flask_jwt_extended import jwt_required, get_jwt_identity



from dotenv import load_dotenv
load_dotenv()



# יצירת אפליקציה
app = Flask(__name__)

app.config['JWT_TOKEN_LOCATION'] = ['headers']
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = timedelta(minutes=15)  # אפשר גם פחות לצורך ניסוי
app.config['JWT_REFRESH_TOKEN_EXPIRES'] = timedelta(days=30)   # טוקן רענון מחזיק יותר זמן
# הגדרת המפתח הסודי (JWT Secret Key)
app.config['JWT_SECRET_KEY'] = os.urandom(24)  # שנה את המפתח הזה למשהו מאובטח

# אתחול של JWT Manager
jwt = JWTManager(app)


swagger = Swagger(app)

# התחברות ל-S3
try:
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name='eu-north-1'
    )
    s3.list_buckets()
    print(" S3 connection successful")
except Exception as e:
    print(" S3 connection failed:", e)

bucket_name = 'messagesystem-omrimeroz'

# התחברות ל-Kafka
try:
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print(" Kafka connection successful")
except Exception as e:
    print(" Kafka connection failed:", e)

@app.route('/messages', methods=['POST'])
def send_message():
    """
    שולח הודעה ל-Kafka ושומר אותה ב-S3
    ---
    parameters:
      - name: message
        in: body
        required: true
        schema:
          type: object
          properties:
            message:
              type: string
    responses:
      200:
        description: ההודעה נשלחה ונותרה ב-S3
    """
    data = request.get_json()
    message = data.get('message')

    if not message:
        return jsonify({'error': 'Missing message'}), 400

    # שליחה ל-Kafka
    try:
        producer.send('message_topic', {'message': message})
        print(" Sent message to Kafka")
    except Exception as e:
        print(" Failed to send to Kafka:", e)

    # שמירה ב-S3
    try:
        file_name = f"message_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
        s3.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=json.dumps({'message': message}),
            ContentType='application/json'
        )
        print(f" Saved message to S3: {file_name}")
    except Exception as e:
        print(" Failed to save to S3:", e)

    return jsonify({'status': 'sent', 'message': message}), 200
# דמוי-DB לאחסון משתמשים (באמת צריך להשתמש ב-database או ב-memory)
users_db = {}

# endpoint להרשמה
@app.route('/register', methods=['POST'])
def register():
    """
    הרשמה של משתמש חדש
    ---
    parameters:
      - name: username
        in: body
        required: true
        type: string
      - name: password
        in: body
        required: true
        type: string
    responses:
      201:
        description: המשתמש נרשם בהצלחה
    """
    # קבלת פרטי המשתמש
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')

    if not username or not password:
        return jsonify({"msg": "Username and password are required"}), 400

    # שמירת המשתמש ב-dummy DB
    if username in users_db:
        return jsonify({"msg": "User already exists"}), 400
    
    # כאן ניתן להוסיף הצפנה לסיסמה (לא מומלץ לשמור סיסמה plain text)
    users_db[username] = password
    return jsonify({"msg": "User registered successfully"}), 201


# endpoint של Login (כניסת משתמש)
from flask_jwt_extended import create_access_token, create_refresh_token

@app.route('/login', methods=['POST'])
def login():
    """
    התחברות של משתמש
    ---
    requestBody:
      required: true
      content:
        application/json:
          schema:
            type: object
            properties:
              username:
                type: string
              password:
                type: string
    responses:
      200:
        description: חזרה עם JWT אם ההתחברות הצליחה
        content:
          application/json:
            schema:
              type: object
              properties:
                access_token:
                  type: string
                refresh_token:
                  type: string
    """
    # קבלת פרטי המשתמש
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')

    if not username or not password:
        return jsonify({"msg": "Username and password are required"}), 400

    # אימות המשתמש
    stored_password = users_db.get(username)
    if stored_password is None or stored_password != password:
        return jsonify({"msg": "Bad username or password"}), 401

    # יצירת JWT
    access_token = create_access_token(identity=username)
    refresh_token = create_refresh_token(identity=username)

    return jsonify(access_token=access_token, refresh_token=refresh_token), 200

# דוגמת endpoint מוגן
@app.route('/protected', methods=['GET'])
@jwt_required()  # דואג לכך רק משתמש עם JWT תקף יוכל לגשת
def protected():
    current_user = get_jwt_identity()
    return jsonify(logged_in_as=current_user), 200

@app.route('/refresh', methods=['POST'])
@jwt_required(refresh=True)
def refresh():
    """
    חידוש Access Token עם Refresh Token
    ---
    security:
      - bearerAuth: []
    responses:
      200:
        description: Access token חדש
        content:
          application/json:
            schema:
              type: object
              properties:
                access_token:
                  type: string
    """
    current_user = get_jwt_identity()
    new_access_token = create_access_token(identity=current_user)
    return jsonify(access_token=new_access_token), 200

if __name__ == '__main__':
    app.run(debug=True)
