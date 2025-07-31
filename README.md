# DOL Analytics API

A FastAPI backend that analyzes Department of Labor (DOL) application processing data to visualize trends, generate metrics, and provide predictions for case completion times.

## Features

- **Data Visualization**: Track daily, weekly, and monthly processing volumes
- **Analytics Dashboard**: Monitor current progress and trends in DOL processing
- **Backlog Analysis**: Track ANALYST REVIEW queue size over time
- **Prediction Engine**: Estimate case completion times based on historical data and current backlog
- **RESTful API**: Easily integrate with frontend applications

## Technology Stack

- **FastAPI**: High-performance web framework
- **PostgreSQL**: Robust, scalable database (hosted on Railway)
- **Python 3.8+**: Modern Python with type hints
- **Docker**: Containerization for easy deployment
- **GitHub Actions**: CI/CD pipeline

## Getting Started

### Prerequisites

- Python 3.8 or higher
- PostgreSQL database

### Environment Variables

Create a `.env` file in the project root with the following variables:

```
# Database Configuration
POSTGRES_DATABASE_URL=postgresql://username:password@hostname:port/database

# Application Configuration
DEBUG=True
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
- `GET /api/data/monthly-backlog` - Get monthly backlog data showing backlog (ANALYST REVIEW + RECONSIDERATION APPEALS), WITHDRAWN, DENIED, and RFI cases
- `GET /api/data/todays-progress` - Get today's progress metrics
- `GET /api/data/processing-times` - Get the latest processing time metrics

### Predictions

- `POST /api/predictions/processing-time` - Predict completion date based on submission date
- `POST /api/predictions/case` - Predict completion for a specific case

## Database Structure

The service uses a PostgreSQL database with the following tables and views:

- **daily_progress**: Daily processing statistics
- **monthly_status**: Monthly case counts by status type
- **processing_times**: Statistical measures of processing durations
- **summary_stats**: Daily summary of application processing
- **weekly_summary**: Weekly aggregation of processing data (view)
- **monthly_summary**: Monthly aggregation of processing data (view)

## Project Structure

```
dol-analytics/
├── src/
│   └── dol_analytics/
│       ├── main.py                  # FastAPI app entry point
│       ├── config.py                # Configuration settings
│       ├── models/
│       │   ├── database.py          # PostgreSQL connection management
│       │   ├── database_docs.py     # Database documentation
│       │   └── schemas.py           # Pydantic models
│       └── api/
│           └── routes/
│               ├── data.py          # Data retrieval endpoints
│               └── predictions.py   # Prediction endpoints
├── tests/                           # Test suite
├── .github/                         # GitHub Actions workflows
└── requirements.txt                 # Python dependencies
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.