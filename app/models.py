from datetime import datetime
from pydantic import BaseModel, Field


class PackageScanRequest(BaseModel):
    piece_id: str
    robot_id: str
    camera_id: str
    barcode: str


class ReviewRequest(BaseModel):
    action: str
    route: str
    operator_id: str


class PackageState(BaseModel):
    piece_id: str
    status: str
    barcode: str
    robot_id: str
    camera_id: str
    route: str | None = None
    history: list[str] = Field(default_factory=list)
    enrichment_metadata: dict[str, object] = Field(default_factory=dict)
    retry_count: int = 0
    created_at: datetime
    updated_at: datetime
    operator_id: str | None = None


class RobotInfo(BaseModel):
    robot_id: str
    last_seen: str
    stale: bool


class CameraInfo(BaseModel):
    camera_id: str
    last_seen: str
    stale: bool


class StatsResponse(BaseModel):
    total: int
    by_status: dict[str, int]
    error_rate: float


class HealthResponse(BaseModel):
    status: str
    redis: str
