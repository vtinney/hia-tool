from datetime import datetime

from sqlalchemy import JSON, Boolean, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from backend.models.database import Base


class Template(Base):
    __tablename__ = "templates"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    user_id: Mapped[str] = mapped_column(String(64), default="local", index=True)
    name: Mapped[str] = mapped_column(String(255))
    description: Mapped[str | None] = mapped_column(Text, default=None)
    config_json: Mapped[dict | None] = mapped_column(JSON, default=None)
    is_builtin: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(
        insert_default=func.now(),
    )
