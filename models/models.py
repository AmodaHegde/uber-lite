from datetime import datetime

from pydantic import BaseModel, Field


class HighVolumeFHVTrip(BaseModel):
    # Base info
    hvfhs_license_num: str | None = Field(
        None, description="High Volume FHV license number"
    )
    dispatching_base_num: str | None = Field(
        None, description="TLC base license number"
    )
    originating_base_num: str | None = Field(
        None, description="Base receiving original request"
    )

    # Timestamps
    request_datetime: datetime | None = None
    on_scene_datetime: datetime | None = None
    pickup_datetime: datetime | None = None
    dropoff_datetime: datetime | None = None

    # Locations & Distance
    pulocationid: int | None = Field(None, ge=0, description="Pickup Taxi Zone ID")
    dolocationid: int | None = Field(None, ge=0, description="Dropoff Taxi Zone ID")
    trip_miles: float | None = Field(None, ge=0.0)
    trip_time: int | None = Field(None, ge=0)

    # Financials
    base_passenger_fare: float | None = Field(None, ge=0.0)
    tolls: float | None = Field(None, ge=0.0)
    bcf: float | None = Field(None, ge=0.0)
    sales_tax: float | None = Field(None, ge=0.0)
    congestion_surcharge: float | None = Field(None, ge=0.0)
    airport_fee: float | None = Field(None, ge=0.0)
    tips: float | None = Field(None, ge=0.0)
    driver_pay: float | None = Field(None)

    # Flags
    shared_request_flag: str | None = None
    shared_match_flag: str | None = None
    access_a_ride_flag: str | None = None
    wav_request_flag: str | None = None
    wav_match_flag: str | None = None
