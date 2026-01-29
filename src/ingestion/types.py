from __future__ import annotations

from datetime import datetime
from typing import Any, Optional, Tuple

from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator

def _parse_dt(value: Any) -> Optional[datetime]:
    """Parse Socrata timestamps:
    -   '2026-01-24T20:32:02.000' (no timezone)
    -   '2026-01-24T21:11:04.833Z' (UTC)
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    
    s = str(value).strip()
    if not s:
        return None
    
    # Handle trailing Z (UTC)
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    
    # datetime.fromisoformat handles both 'YYYY-MM-DDTHH:MM:SS.mmm'
    # and "...+00:00"
    return datetime.fromisoformat(s)

class SocrataPoint(BaseModel):
        model_config = ConfigDict(extra="ignore")

        type: str
        coordinates: Tuple[float, float] #(lon, lat)

        @field_validator("type")
        @classmethod
        def validate_type(cls, v: str) -> str:
             v2 = v.strip()
             if v2.lower() != "point":
                  raise ValueError(f"Expected point geomatry, got {v2}")
             return "Point"
        
        @field_validator("coordinates")
        @classmethod
        def validate_coords(cls, v: Tuple[float, float]) -> Tuple[float, float]:
             lon, lat = v
             if not (-180 <= lon <= 180):
                  raise ValueError(f"Invalid longitude: {lon}")
             if not (-90 <= lat <= 90):
                  raise ValueError(f"Invalid latitude: {lat}")
             return (lon, lat)
        

class TrafficIncidentRow(BaseModel):
     """
     Represents ONE row returned fromt he Socrata SODA3 endpoint.
     Keeps business fields + a few system fields used for incremental loading.
     """

     model_config = ConfigDict(extra='allow') # allow computed_region fields, etc.

     # Business fields
     incident_info: str
     description: Optional[str] = None
     start_dt: datetime
     modified_dt: Optional[datetime] = None
     quadrant: Optional[str] = None
     longitude: float
     latitude: float
     count: int = 1
     id: str    # business key (incident id)
     point: Optional[SocrataPoint] = None


     # System fields (Socrata)
     socrata_row_id: Optional[str] = Field(default=None, alias=":id")
     socrata_version: Optional[str] = Field(default=None, alias=":version")
     socrata_created_at: Optional[datetime] = Field(default=None, alias=":created_at")
     socrata_updated_at: Optional[datetime] = Field(default=None, alias=":updated_at")

     
     # ----    Validators / normalizers    ----

     @field_validator("incident_info", mode="before")
     @classmethod
     def normalize_incident_info(cls, v: Any) -> str:
          s = str(v or "")
          # incident_info data has leading/trailing spaces
          s = " ".join(s.split())
          if not s:
               raise ValueError("incident_info is empty")
          return s
     
     @field_validator("quadrant", mode="before")
     @classmethod
     def normalize_quadrant(cls, v: Any) -> Optional[str]:
          if v is None:
               return None
          s = str(v).strip().upper()
          return s or None
     
     @field_validator("longitude", "latitude", mode="before")
     @classmethod
     def cast_float(cls, v: Any) -> float:
          if v is None:
               raise ValueError("Missing coordinate")
          return float(str(v).strip())
     
     @field_validator("count", mode="before")
     @classmethod
     def cast_int(cls, v: Any) -> int:
          if v is None:
               return 1
          return int(float(str(v).strip()))
     
     @field_validator("start_dt", "modified_dt", "socrata_created_at", "socrata_updated_at", mode="before")
     @classmethod
     def parse_datetimes(cls, v: Any) -> Optional[datetime]:
          return _parse_dt(v)
     
     @model_validator(mode="after")
     def cross_validate_coords(self) -> "TrafficIncidentRow":
          # if point exists, ensure it matches longitude/latitude closely.
          if self.point is not None:
               lon_p, lat_p = self.point.coordinates
               # allow tiny floating differences
               if abs(lon_p - self.longitude) > 1e-8 or abs(lat_p - self.latitude) > 1e-8:
                    raise ValueError("point.coordinates do not match longitude/latitude")
          return self