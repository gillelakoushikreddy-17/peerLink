FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy server files
COPY server.py protocol.py config.py ./

# Expose default signaling port
EXPOSE 5050

# Run signaling server on all interfaces
CMD ["python", "server.py", "0.0.0.0", "5050"]
