import io
import runpy
import subprocess
import sys
from contextlib import redirect_stdout
from pathlib import Path

from src.aiops_pipeline import load_data, run_pipeline
from src.anomaly_detector import AnomalyDetector
from src.event_consumer import EventConsumer
from src.event_producer import EventProducer
from src.event_topic import EventTopic

ROOT = Path(__file__).resolve().parents[1]
DATA_FILE = ROOT / "data" / "service_data.json"


def test_normal_record_is_not_anomaly():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:00:00",
        "service": "payment-service",
        "response_time_ms": 120,
        "cpu_percent": 42,
        "memory_percent": 51,
        "log_level": "INFO",
        "message": "Payment request processed successfully"
    }

    assert detector.detect(record) is None


def test_anomalous_record_is_detected():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:05:00",
        "service": "payment-service",
        "response_time_ms": 610,
        "cpu_percent": 75,
        "memory_percent": 70,
        "log_level": "ERROR",
        "message": "Payment service timeout"
    }

    event = detector.detect(record)

    assert event is not None
    assert event["type"] == "ANOMALY"


def test_producer_publishes_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)

    event = {
        "type": "ANOMALY",
        "service": "payment-service"
    }

    assert producer.publish(event)
    assert len(topic.get_messages()) == 1


def test_consumer_receives_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)
    consumer = EventConsumer(topic)

    event = {
        "type": "ANOMALY",
        "service": "payment-service"
    }

    producer.publish(event)

    messages = consumer.consume()

    assert len(messages) == 1


def test_load_data_reads_json_file():
    data = load_data(DATA_FILE)

    assert isinstance(data, list)
    assert len(data) == 10
    assert data[0]["service"] == "payment-service"


def test_run_pipeline_returns_detected_anomalies_and_consumed_events():
    result = run_pipeline(str(DATA_FILE))

    assert result["records_processed"] == 10
    assert len(result["anomalies_detected"]) == 2
    assert len(result["events_consumed"]) == 2
    assert result["events_consumed"][0]["service"] == "payment-service"


def test_warning_log_is_detected_as_anomaly():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:15:00",
        "service": "checkout-service",
        "response_time_ms": 120,
        "cpu_percent": 50,
        "memory_percent": 55,
        "log_level": "WARNING",
        "message": "Timeout warning logged"
    }

    event = detector.detect(record)

    assert event is not None
    assert "Error log detected" in event["reasons"]


def test_producer_rejects_empty_event_and_topic_can_clear_messages():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)

    assert producer.publish({}) is False
    assert topic.get_messages() == []

    event = {"type": "ANOMALY", "service": "payment-service"}
    producer.publish(event)
    topic.clear()

    assert topic.get_messages() == []


def test_aiops_pipeline_main_entrypoint_runs_successfully():
    result = subprocess.run(
        [sys.executable, "src/aiops_pipeline.py"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "Records processed: 10" in result.stdout
    assert "Anomalies detected: 2" in result.stdout
    assert "Events consumed: 2" in result.stdout


def test_aiops_pipeline_main_block_runs_under_coverage():
    buffer = io.StringIO()

    with redirect_stdout(buffer):
        runpy.run_path(str(ROOT / "src" / "aiops_pipeline.py"), run_name="__main__")

    output = buffer.getvalue()
    assert "AIOps Pipeline Result" in output
    assert "Records processed: 10" in output
    assert "Anomalies detected: 2" in output
    assert "Events consumed: 2" in output