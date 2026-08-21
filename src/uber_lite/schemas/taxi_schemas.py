from dataclasses import dataclass, field
from datetime import UTC, datetime


@dataclass
class DriverTelemetry:
    # Identifiers
    trip_id: str
    taxi_id: str

    # Timestamps & Durations
    trip_start_timestamp: str | None = None
    trip_end_timestamp: str | None = None
    trip_seconds: int | None = None
    trip_miles: float | None = None

    # Location Information
    pickup_census_tract: str | None = None
    dropoff_census_tract: str | None = None
    pickup_community_area: int | None = None
    dropoff_community_area: int | None = None

    # Centroid Coordinates
    pickup_centroid_latitude: float | None = None
    pickup_centroid_longitude: float | None = None
    dropoff_centroid_latitude: float | None = None
    dropoff_centroid_longitude: float | None = None

    # Financial Details
    fare: float | None = None
    tips: float | None = None
    tolls: float | None = None
    extras: float | None = None
    trip_total: float | None = None

    # Payment & Company Metadata
    payment_type: str | None = None
    company: str | None = None

    # System Tracking Field
    ingestion_timestamp: str = field(
        default_factory=lambda: datetime.now(tz=UTC).isoformat()
    )

    def to_dict(self) -> dict:
        return self.__dict__
