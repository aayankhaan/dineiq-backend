from enum import Enum

from sqlmodel import SQLModel, Field
from datetime import datetime, timezone

class UserRole(str, Enum):
    admin = "admin"
    regional_manager = "regional_manager"
    restaurant_manager = "restaurant_manager"
    analyst = "analyst"


class User(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str
    email: str = Field(unique=True, index=True)
    password_hash: str
    role: UserRole = UserRole.analyst
    is_active: bool = True
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class UserRead(SQLModel):
    id: int
    name: str
    email: str
    role: UserRole
    is_active: bool
    created_at: datetime
