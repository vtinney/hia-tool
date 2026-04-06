from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.database import get_db
from backend.models.template import Template

# ── Pydantic schemas ────────────────────────────────────────────────

class TemplateCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: str | None = None
    config: dict | None = Field(None, alias="config")


class TemplateOut(BaseModel):
    id: int
    name: str
    description: str | None
    config_json: dict | None
    is_builtin: bool
    created_at: datetime

    model_config = {"from_attributes": True}


# ── Router ──────────────────────────────────────────────────────────

router = APIRouter(prefix="/api", tags=["templates"])


@router.post("/templates", response_model=TemplateOut, status_code=201)
async def create_template(
    body: TemplateCreate,
    db: AsyncSession = Depends(get_db),
) -> Template:
    template = Template(
        name=body.name,
        description=body.description,
        config_json=body.config,
        is_builtin=False,
    )
    db.add(template)
    await db.commit()
    await db.refresh(template)
    return template


@router.get("/templates", response_model=list[TemplateOut])
async def list_templates(
    db: AsyncSession = Depends(get_db),
) -> list[Template]:
    result = await db.execute(
        select(Template).order_by(Template.is_builtin.desc(), Template.created_at.desc())
    )
    return list(result.scalars().all())


@router.get("/templates/{template_id}", response_model=TemplateOut)
async def get_template(
    template_id: int,
    db: AsyncSession = Depends(get_db),
) -> Template:
    result = await db.execute(select(Template).where(Template.id == template_id))
    template = result.scalar_one_or_none()
    if template is None:
        raise HTTPException(status_code=404, detail="Template not found")
    return template


@router.delete("/templates/{template_id}", status_code=204)
async def delete_template(
    template_id: int,
    db: AsyncSession = Depends(get_db),
) -> None:
    result = await db.execute(select(Template).where(Template.id == template_id))
    template = result.scalar_one_or_none()
    if template is None:
        raise HTTPException(status_code=404, detail="Template not found")
    if template.is_builtin:
        raise HTTPException(status_code=403, detail="Built-in templates cannot be deleted")
    db.delete(template)
    await db.commit()
