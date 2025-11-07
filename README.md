<M2 Money Supply Visualization Pipeline>
A containerized data pipeline that fetches, processes, and visualizes U.S. M2 Money Supply data from the Federal Reserve Economic Data (FRED). Built with Docker, Kafka, and Python for real-time data streaming and interactive visualizations.

<Overview>
This project provides:

1. Automated data retrieval from FRED's WM2NS (Weekly M2 Money Supply) dataset
2. Interactive visualizations highlighting major economic crises (1980-2025)
3. Kafka streaming integration for real-time data event publishing
4. Jenkins CI/CD pipeline for automated builds and deployments
5. Docker orchestration with Zookeeper, Kafka, and Python application services


<Key Metrics Calculated>
M2 is not seasonally-adjusted.
Total Growth: Percentage growth since data start
CAGR: Compound Annual Growth Rate
YoY Change: Year-over-year percentage change (52-week periods)


<Docker Services>
Zookeeper
    Port: 2181
    Purpose: Kafka cluster coordination

Kafka
    Internal Port: 29092 (container-to-container)
    External Port: 9092 (host access)
    Purpose: Event streaming for data updates

M2 Visualization
    Base Image: python:3.11-slim
    Volumes: ./output:/app/output
    Purpose: Data processing and visualization