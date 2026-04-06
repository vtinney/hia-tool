from datetime import datetime

from sqlalchemy import JSON, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from backend.models.database import Base


class FileUpload(Base):
    __tablename__ = "file_uploads"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    user_id: Mapped[str] = mapped_column(String(64), default="local", index=True)
    original_filename: Mapped[str] = mapped_column(String(512))
    stored_filename: Mapped[str] = mapped_column(String(512), unique=True)
    file_type: Mapped[str] = mapped_column(
        String(32),
    )  # geotiff | shapefile | geopackage | geojson | csv
    category: Mapped[str] = mapped_column(
        String(32), index=True
    )  # concentration | population | boundary
    file_size_bytes: Mapped[int] = mapped_column()
    crs: Mapped[str | None] = mapped_column(String(128), default=None)
    bounds_json: Mapped[dict | None] = mapped_column(JSON, default=None)
    metadata_json: Mapped[dict | None] = mapped_column(JSON, default=None)
    status: Mapped[str] = mapped_column(
        String(32), default="uploaded"
    )  # uploaded | validated | error
    error_message: Mapped[str | None] = mapped_column(Text, default=None)
    created_at: Mapped[datetime] = mapped_column(insert_default=func.now())
