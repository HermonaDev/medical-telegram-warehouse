# Development & Contribution Guidelines

## 🛠 Setup
1. Clone the repo and install requirements: \`pip install -r requirements.txt\`
2. Set up your \`.env\` file based on \`.env.example\`.
3. Start the warehouse: \`docker-compose up -d\`.

## 🧪 Testing Standards
- All new scripts must include type hints.
- Run \`pytest tests/\` to verify data ingestion logic.
- Run \`dbt test\` to ensure warehouse integrity.