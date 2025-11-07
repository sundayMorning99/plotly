import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
import json
import logging
from kafka import KafkaProducer
import os

logging.basicConfig(level=logging.INFO)
logging.getLogger('kafka').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

class M2DataPipeline:
    def __init__(self, kafka_enabled=False):
        self.kafka_enabled = kafka_enabled
        self.producer = None
        if kafka_enabled:
            self.setup_kafka()
    
    def setup_kafka(self):
        try:
            kafka_host = os.getenv('KAFKA_HOST', 'localhost:9092')
            self.producer = KafkaProducer(
                bootstrap_servers=[kafka_host],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info(f"Kafka producer connected to {kafka_host}")
        except Exception as e:
            logger.warning(f"Kafka connection failed: {e}. Running without Kafka.")
            self.kafka_enabled = False
    
    def publish_to_kafka(self, topic, data):
        if self.kafka_enabled and self.producer:
            try:
                self.producer.send(topic, data)
                self.producer.flush()
                logger.info(f"Published to Kafka topic: {topic}")
            except Exception as e:
                logger.error(f"Failed to publish to Kafka: {e}")
    
    def fetch_m2_data_direct(self):
        """Load WM2NS.csv from output/ or download fresh from FRED (NO SAMPLE FALLBACK)"""
        local_file = "/app/output/WM2NS.csv"
        
        # Try local file first
        if os.path.exists(local_file):
            try:
                df = pd.read_csv(local_file, parse_dates=['observation_date'], 
                               usecols=['observation_date', 'WM2NS'])
                df = df.rename(columns={'observation_date': 'date', 'WM2NS': 'value'}).dropna()
                df = df.sort_values('date').reset_index(drop=True)
                logger.info(f"Loaded {len(df)} weekly records from local WM2NS.csv | "
                          f"Latest: ${df['value'].iloc[-1]:,.1f}B ({df['date'].iloc[-1].strftime('%Y-%m-%d')})")
                
                if self.kafka_enabled:
                    self.publish_to_kafka('m2-data-updates', {
                        'source': 'local_csv',
                        'records': len(df),
                        'latest_value': float(df['value'].iloc[-1]),
                        'latest_date': df['date'].iloc[-1].isoformat()
                    })
                return df
            except Exception as e:
                logger.error(f"Local CSV failed: {e}. Trying online download...")
        
        # Try downloading fresh CSV
        url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=WM2NS"
        try:
            logger.info("Downloading latest WM2NS data from FRED...")
            df = pd.read_csv(url, parse_dates=['observation_date'], 
                           usecols=['observation_date', 'WM2NS'])
            df = df.rename(columns={'observation_date': 'date', 'WM2NS': 'value'}).dropna()
            df = df.sort_values('date').reset_index(drop=True)

            os.makedirs(os.path.dirname(local_file), exist_ok=True)
            df.to_csv(local_file, index=False)
            logger.info(f"Loaded {len(df)} weekly records from FRED | "
                      f"Latest: ${df['value'].iloc[-1]:,.1f}B ({df['date'].iloc[-1].strftime('%Y-%m-%d')})")
            
            if self.kafka_enabled:
                self.publish_to_kafka('m2-data-updates', {
                    'source': 'fred_csv_direct',
                    'records': len(df),
                    'latest_value': float(df['value'].iloc[-1]),
                    'latest_date': df['date'].iloc[-1].isoformat()
                })
            return df
        except Exception as e:
            raise RuntimeError(f"ALL DATA SOURCES FAILED: {e}. Place WM2NS.csv in output/ folder.") 
    
    def create_visualization(self, df, output_file='m2_visualization.html'):
        crisis_periods = [
            {'name': 'S&L Crisis', 'start': datetime(1989, 1, 1), 'end': datetime(1991, 12, 31)},
            {'name': 'Dot-com Bubble', 'start': datetime(2000, 3, 1), 'end': datetime(2002, 10, 31)},
            {'name': 'Financial Crisis', 'start': datetime(2007, 12, 1), 'end': datetime(2009, 6, 30)},
            {'name': 'COVID-19 Pandemic', 'start': datetime(2020, 2, 1), 'end': datetime(2021, 12, 31)}
        ]
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df['date'], y=df['value'],
            mode='lines', name='M2 Money Supply (Weekly)',
            line=dict(color='#2563eb', width=2),
            hovertemplate='<b>%{x|%Y-%m-%d}</b><br><b>M2:</b> $%{y:,.0f}B<extra></extra>'
        ))
        
        for crisis in crisis_periods:
            fig.add_vrect(
                x0=crisis['start'], x1=crisis['end'],
                fillcolor="gray", opacity=0.15, layer="below", line_width=0,
                annotation_text=crisis['name'], annotation_position="top left",
                annotation=dict(font_size=10, font_color="gray")
            )
        
        fig.update_layout(
            title="U.S. M2 Money Supply (1980-2025) - Weekly WM2NS (Not Seasonally Adjusted)",
            xaxis_title="Date", yaxis_title="M2 (Billions USD)",
            hovermode='x unified', height=700,
            plot_bgcolor='white', paper_bgcolor='white'
        )

        full_path = f"/app/output/{output_file}"
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        
        fig.write_html(full_path)
        logger.info(f"Chart saved: {full_path}")
        
        if self.kafka_enabled:
            self.publish_to_kafka('visualization-complete', {
                'file': output_file,
                'status': 'success',
                'timestamp': datetime.now().isoformat()
            })
        return fig
    
    def calculate_statistics(self, df):
        if len(df) >= 52:
            df['yoy'] = df['value'].pct_change(52) * 100
        else:
            logger.warning(f"Insufficient data for YoY calculation: {len(df)} weeks")
            df['yoy'] = None
        
        years = (df['date'].max() - df['date'].min()).days / 365.25
        cagr = ((df['value'].iloc[-1] / df['value'].iloc[0]) ** (1/years) - 1) * 100
        return {
            'current': df['value'].iloc[-1],
            'growth': (df['value'].iloc[-1] / df['value'].iloc[0] - 1) * 100,
            'cagr': cagr
        }
    
    def __del__(self):
        """Cleanup Kafka producer"""
        if self.producer:
            try:
                self.producer.close()
            except Exception:
                pass

def main():
    logger.info("Starting M2 Pipeline - REAL WEEKLY WM2NS DATA")
    kafka_enabled = os.getenv('ENABLE_KAFKA', 'false').lower() == 'true'
    pipeline = M2DataPipeline(kafka_enabled=kafka_enabled)
    
    os.makedirs("/app/output", exist_ok=True)
    
    df = pipeline.fetch_m2_data_direct()
    stats = pipeline.calculate_statistics(df)
    
    logger.info(f"Latest M2: ${stats['current']:,.1f}B ({df['date'].iloc[-1].strftime('%Y-%m-%d')}) | "
                f"Total Growth: {stats['growth']:.1f}% | CAGR: {stats['cagr']:.2f}%")
    
    pipeline.create_visualization(df)
    df.to_csv('/app/output/m2_data.csv', index=False)
    logger.info("Pipeline completed successfully!")
    logger.info("Output files: m2_visualization.html, m2_data.csv")

if __name__ == '__main__':
    main()