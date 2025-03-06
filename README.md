# DOL Analytics API

A FastAPI backend that utilizes the Department of Labor (DOL) API to visualize data, generate metrics, and provide predictions for case completion times.

## Features

- **Data Visualization**: Track daily, weekly, and monthly volumes
- **Analytics Dashboard**: Monitor current progress and trends
- **Prediction Engine**: Estimate case completion times based on historical data and current backlog
- **Scheduled Data Fetching**: Automatically fetch and process DOL API data
- **RESTful API**: Easily integrate with frontend applications

## Technology Stack

- **FastAPI**: High-performance web framework
- **PostgreSQL**: Robust, scalable database (hosted on Railway)
- **SQLAlchemy**: SQL toolkit and ORM
- **APScheduler**: Task scheduling for cron jobs
- **Docker**: Containerization for easy deployment
- **GitHub Actions**: CI/CD pipeline

## Getting Started

### Prerequisites

- Python 3.12 or higher
- Docker and Docker Compose (for local development with containers)
- DOL API key (register at https://dataportal.dol.gov/registration)

### Environment Variables

Create a `.env` file in the project root with the following variables:

```
DOL_API_KEY=your_api_key_here
DOL_AGENCY=agency_abbreviation
DOL_ENDPOINT=api_endpoint
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/dol_analytics
DEBUG=True
DATA_FETCH_INTERVAL=60
```

### Local Development

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/dol-analytics.git
   cd dol-analytics
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements-dev.txt
   ```

3. Run locally:
   ```bash
   uvicorn src.dol_analytics.main:app --reload
   ```

### Using Docker

1. Build and start the containers:
   ```bash
   docker-compose up -d
   ```

2. The API will be available at http://localhost:8000

### API Documentation

Once the application is running, you can access:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## Deployment

This project is configured to deploy to Railway via GitHub Actions.

1. Push changes to the main branch.
2. GitHub Actions will run tests and deploy to Railway if tests pass.

## API Endpoints

### Data Visualization

- `GET /api/data/dashboard` - Get comprehensive dashboard data
- `GET /api/data/daily-volume` - Get daily volume data
- `GET /api/data/weekly-averages` - Get average volume by day of week
- `GET /api/data/weekly-volumes` - Get weekly volume totals
- `GET /api/data/monthly-volumes` - Get monthly volume data
- `GET /api/data/todays-progress` - Get today's progress metrics
- `POST /api/data/refresh` - Manually trigger data refresh

### Predictions

- `GET /api/predictions/case/{case_id}` - Predict completion date for a specific case
- `GET /api/predictions/from-date` - Predict completion date for a hypothetical case
- `GET /api/predictions/expected-time` - Get current expected processing time

## Project Structure

```
dol-analytics/
├── src/
│   └── dol_analytics/
│       ├── main.py               # FastAPI app entry point
│       ├── config.py             # Configuration settings
│       ├── models/               # Database models and schemas
│       ├── api/                  # API routes
│       ├── services/             # Business logic
│       └── tasks/                # Scheduler for cron jobs
├── tests/                        # Test suite
├── .github/                      # GitHub Actions workflows
├── Dockerfile                    # Docker configuration
├── docker-compose.yml            # Docker Compose configuration
└── requirements.txt              # Python dependencies
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.