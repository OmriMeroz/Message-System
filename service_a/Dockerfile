# Base image - לוקח תמונת בסיס עם Python
FROM python:3.10-slim

# Set working directory בתוך הקונטיינר
WORKDIR /app

# מעתיק את הקבצים לתוך הקונטיינר
COPY . .

# מתקין את התלויות מהקובץ requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# מציין את הפורט שהאפליקציה מריצה עליו
EXPOSE 5000

# הפקודה שמריצה את Flask
CMD ["python", "app.py"]
