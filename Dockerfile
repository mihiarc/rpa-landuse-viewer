FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
COPY pyproject.toml ./
COPY README.md ./

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install -e .

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "app.py"] 