FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY node ./node

EXPOSE 8000
HEALTHCHECK --interval=5s --timeout=2s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health', timeout=1)"

CMD ["uvicorn", "node.main:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "warning"]
