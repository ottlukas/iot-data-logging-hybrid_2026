import asyncio
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Iterable, List, Optional, Tuple

import aiofiles
from fastapi import HTTPException
from pydantic import BaseModel

from app.iotdb_client import IoTDBClient
from app.models import SensorReading


class IoTDBConfig(BaseModel):
    host: str
    port: int
    username: str
    password: str
    zoneId: Optional[str] = None


class SensorDataRequest(BaseModel):
    iotdbConfig: Optional[IoTDBConfig] = None
    devicePath: Optional[str] = None
    tsfilePaths: List[str] = []
    measurements: List[str]
    startISO: str
    endISO: str
    resampleIntervalMs: Optional[int] = None
    downsample: Optional[str] = "avg"
    preferSource: Optional[str] = "iotdb"
    chartOptions: Optional[Dict[str, Any]] = None


def parse_iso_to_epoch_ms(timestamp_iso: str) -> Tuple[int, str]:
    if timestamp_iso.endswith("Z"):
        timestamp_iso = timestamp_iso[:-1] + "+00:00"
    dt = datetime.fromisoformat(timestamp_iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    epoch_ms = int(dt.timestamp() * 1000)
    normalized = dt.isoformat().replace("+00:00", "Z")
    return epoch_ms, normalized


def normalize_numeric(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def measurement_series_name(measurement: str, source: str) -> str:
    return f"{measurement}_{source}"


def bucket_key(timestamp: int, start_ms: int, interval_ms: int) -> int:
    return (timestamp - start_ms) // interval_ms


def aggregate_bucket(values: List[Optional[float]], method: str) -> Optional[float]:
    non_null = [v for v in values if v is not None]
    if not non_null:
        return None
    if method == "min":
        return min(non_null)
    if method == "max":
        return max(non_null)
    if method == "sum":
        return sum(non_null)
    return sum(non_null) / len(non_null)


async def query_iotdb_points(
    iotdb_config: IoTDBConfig,
    device_path: str,
    measurements: List[str],
    start_ms: int,
    end_ms: int,
) -> AsyncIterator[Dict[str, Any]]:
    client = IoTDBClient(
        host=iotdb_config.host,
        port=iotdb_config.port,
        user=iotdb_config.username,
        password=iotdb_config.password,
        zone_id=iotdb_config.zoneId,
    )
    await client.connect()

    if not measurements:
        await client.close()
        return

    try:
        async for row, col_types in client.query_range(device_path, measurements, start_ms, end_ms):
            timestamp_ms = int(row.get_timestamp())
            original_iso = datetime.fromtimestamp(timestamp_ms / 1000.0, timezone.utc).isoformat().replace("+00:00", "Z")
            fields = row.get_fields() or []
            for idx, measurement in enumerate(measurements):
                field = fields[idx] if idx < len(fields) else None
                # Map field to its corresponding column type (skipping Time at index 0)
                col_type = col_types[idx + 1]
                val = field.get_object_value(col_type) if field is not None else None
                value = normalize_numeric(val)
                yield {
                    "timestamp": timestamp_ms,
                    "measurement": measurement,
                    "value": value,
                    "source": "iotdb",
                    "originalTimestampISO": original_iso,
                    "quality": "ok",
                }
    finally:
        await client.close()


async def read_tsfile_points(
    tsfile_paths: List[str],
    measurements: List[str],
    start_ms: int,
    end_ms: int,
) -> AsyncIterator[Dict[str, Any]]:
    for file_path in tsfile_paths:
        path = Path(file_path)
        if not path.exists():
            continue
        async with aiofiles.open(path, mode="r", encoding="utf-8") as f:
            async for raw in f:
                line = raw.strip()
                if not line:
                    continue
                try:
                    reading = SensorReading.model_validate_json(line)
                except Exception:
                    continue
                timestamp_ms = int(
                    reading.timestamp.astimezone(timezone.utc).timestamp() * 1000
                    if reading.timestamp.tzinfo is not None
                    else reading.timestamp.replace(tzinfo=timezone.utc).timestamp() * 1000
                )
                if timestamp_ms < start_ms or timestamp_ms > end_ms:
                    continue
                original_iso = reading.timestamp.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
                for measurement in measurements:
                    value = normalize_numeric(getattr(reading, measurement, None))
                    yield {
                        "timestamp": timestamp_ms,
                        "measurement": measurement,
                        "value": value,
                        "source": "tsfile",
                        "originalTimestampISO": original_iso,
                        "quality": "ok",
                    }


def build_series_and_rows(
    points: Iterable[Dict[str, Any]],
    measurements: List[str],
    prefer_source: str,
    start_ms: int,
    resample_interval_ms: Optional[int],
    downsample: str,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    points_by_series: Dict[str, Dict[int, Dict[str, Any]]] = defaultdict(dict)

    for point in points:
        series_name = measurement_series_name(point["measurement"], point["source"])
        key = point["timestamp"]
        existing = points_by_series[series_name].get(key)
        if existing is not None:
            existing["quality"] = "dup"
            continue
        points_by_series[series_name][key] = point.copy()

    if resample_interval_ms is not None and resample_interval_ms > 0:
        resampled_by_series: Dict[str, Dict[int, Dict[str, Any]]] = defaultdict(dict)
        for series_name, points_map in points_by_series.items():
            for point in points_map.values():
                bucket_index = bucket_key(point["timestamp"], start_ms, resample_interval_ms)
                bucket_ts = start_ms + bucket_index * resample_interval_ms
                bucket = resampled_by_series[series_name].setdefault(
                    bucket_ts,
                    {
                        "x": bucket_ts,
                        "measurement": point["measurement"],
                        "source": point["source"],
                        "values": [],
                        "originalTimestampISO": point["originalTimestampISO"],
                    },
                )
                bucket["values"].append(point["value"])

        points_by_series = defaultdict(dict)
        for series_name, bucket_map in resampled_by_series.items():
            for bucket_ts, bucket in bucket_map.items():
                aggregated = aggregate_bucket(bucket["values"], downsample)
                points_by_series[series_name][bucket_ts] = {
                    "x": bucket_ts,
                    "y": aggregated,
                    "measurement": bucket["measurement"],
                    "source": bucket["source"],
                    "originalTimestampISO": bucket["originalTimestampISO"],
                    "quality": "ok" if aggregated is not None else "gap",
                }

    series = []
    for series_name, points_map in points_by_series.items():
        data_points = [points_map[t] for t in sorted(points_map)]
        series.append({"name": series_name, "data": data_points})

    table_rows_by_timestamp: Dict[int, Dict[str, Any]] = {}
    for series in series:
        measurement, source = series["name"].rsplit("_", 1)
        for point in series["data"]:
            row = table_rows_by_timestamp.setdefault(point["x"], {"timestamp": point["x"]})
            row[f"{measurement}_{source}"] = point["y"]
            row[f"{measurement}_{source}_originalTimestampISO"] = point["originalTimestampISO"]

    table_rows = []
    for timestamp in sorted(table_rows_by_timestamp):
        row = table_rows_by_timestamp[timestamp]
        overall_quality = "ok"
        for measurement in measurements:
            iotdb_value = row.get(f"{measurement}_iotdb")
            tsfile_value = row.get(f"{measurement}_tsfile")
            if iotdb_value is not None and tsfile_value is not None:
                overall_quality = "dup"
                break
            if iotdb_value is None and tsfile_value is None:
                overall_quality = "gap"
        row["quality"] = overall_quality
        if overall_quality == "dup":
            row["source"] = prefer_source if prefer_source in {"iotdb", "tsfile"} else "iotdb"
        else:
            row["source"] = "iotdb" if any(row.get(f"{m}_iotdb") is not None for m in measurements) else "tsfile"
        table_rows.append(row)

    metadata = {
        "seriesCount": len(series),
        "measurements": measurements,
        "preferSource": prefer_source,
        "resampleIntervalMs": resample_interval_ms,
        "downsample": downsample,
        "rowCount": len(table_rows),
    }

    return series, table_rows, metadata


async def fetch_and_merge_sensor_data(
    iotdb_config: Optional[Dict[str, Any]],
    tsfile_paths: List[str],
    measurements: List[str],
    opts: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    opts = opts or {}
    request = SensorDataRequest(
        iotdbConfig=IoTDBConfig(**iotdb_config) if iotdb_config else None,
        tsfilePaths=tsfile_paths or [],
        measurements=measurements,
        startISO=opts.get("startISO"),
        endISO=opts.get("endISO"),
        resampleIntervalMs=opts.get("resampleIntervalMs"),
        downsample=opts.get("downsample", "avg"),
        preferSource=opts.get("preferSource", "iotdb"),
        chartOptions=opts.get("chartOptions"),
    )

    start_ms, _ = parse_iso_to_epoch_ms(request.startISO)
    end_ms, _ = parse_iso_to_epoch_ms(request.endISO)
    if start_ms > end_ms:
        raise HTTPException(status_code=400, detail="startISO must be before endISO")

    all_points: List[Dict[str, Any]] = []
    errors: List[str] = []

    try:
        async for point in read_tsfile_points(request.tsfilePaths, request.measurements, start_ms, end_ms):
            all_points.append(point)
    except Exception as exc:
        errors.append(f"TSFile read error: {exc}")

    if request.iotdbConfig is not None:
        if not request.devicePath:
            raise HTTPException(status_code=400, detail="devicePath is required when iotdbConfig is provided")
        try:
            async for point in query_iotdb_points(
                request.iotdbConfig,
                request.devicePath,
                request.measurements,
                start_ms,
                end_ms,
            ):
                all_points.append(point)
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"IoTDB query failed: {exc}") from exc

    series, table_rows, metadata = build_series_and_rows(
        all_points,
        request.measurements,
        request.preferSource,
        start_ms,
        request.resampleIntervalMs,
        request.downsample,
    )

    metadata["errors"] = errors
    metadata["chartOptions"] = request.chartOptions or {}
    return {"series": series, "tableRows": table_rows, "metadata": metadata}
