"""
OXYS Ingestion Service
Continuously polls real external data sources (Crypto Market Stream, Weather Telemetry)
and pushes normalized events to Kafka / Stream Processor.
"""
import time
import logging
import threading
from typing import List, Optional

from ingestion.base import DataSource, NormalizedEvent
from ingestion.usgs_source import USGSEarthquakeSource
from ingestion.crypto_source import CryptoMarketSource
from ingestion.weather_source import WeatherTelemetrySource
from ingestion.producer import OxysKafkaProducer
from engine.processor import processor

logger = logging.getLogger("oxys.ingestion.service")


class OxysIngestionService:
    def __init__(
        self,
        sources: Optional[List[DataSource]] = None,
        producer: Optional[OxysKafkaProducer] = None,
        poll_interval_sec: float = 2.0
    ):
        self.sources = sources or [
            USGSEarthquakeSource(),
            CryptoMarketSource(),
            WeatherTelemetrySource()
        ]
        self.producer = producer or OxysKafkaProducer()
        self.poll_interval_sec = poll_interval_sec
        self.is_running = False
        self._thread: Optional[threading.Thread] = None

    def start(self):
        if self.is_running:
            return
        self.is_running = True
        self.producer.connect()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info(f"OXYS Ingestion Service started with {len(self.sources)} real data source(s).")

    def _run_loop(self):
        while self.is_running:
            try:
                for source in self.sources:
                    events = source.fetch_and_normalize()
                    for event in events:
                        # 1. Publish to Kafka
                        pub_result = self.producer.publish_normalized(event)
                        # 2. In standalone/direct execution, dispatch directly to processor
                        if not self.producer.is_connected:
                            processor.process_event(event)

                time.sleep(self.poll_interval_sec)
            except Exception as e:
                logger.error(f"Error in ingestion loop: {e}")
                time.sleep(self.poll_interval_sec)

    def stop(self):
        self.is_running = False
        self.producer.close()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)


ingestion_service = OxysIngestionService()
