from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from pydantic import BaseModel
from sqlmodel import Session, select
from app.models import User, UserRead, UserRole
from app.utils import (
    require_admin,
    pwd_context,
)
from app.email import send_onboarding_email
import secrets
from app.database import get_session

router = APIRouter(
    prefix="/users", tags=["Users"], dependencies=[Depends(require_admin)]
)


class UserUpdate(BaseModel):
    name: str | None = None
    email: str | None = None
    role: UserRole | None = None
    is_active: bool | None = None
    password: str | None = None


class UserCreate(BaseModel):
    name: str
    email: str
    password: str | None = None
    role: UserRole


@router.get("/", response_model=list[UserRead])
def get_users(db: Session = Depends(get_session)):
    users = db.exec(select(User)).all()

    return users


@router.get("/{user_id}", response_model=UserRead)
def get_user(
    user_id: int,
    db: Session = Depends(get_session),
):
    user = db.get(User, user_id)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )

    return user


@router.post("/", response_model=UserRead)
def create_user(
    data: UserCreate,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_session),
):
    existing_user = db.exec(select(User).where(User.email == data.email)).first()
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered",
        )

    temp_password = data.password or secrets.token_urlsafe(9)  # ~12 chars

    new_user = User(
        name=data.name,
        email=data.email,
        password_hash=pwd_context.hash(temp_password),
        role=data.role,
        is_active=True,
        must_change_password=True,
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    background_tasks.add_task(
        send_onboarding_email, new_user.name, new_user.email, temp_password
    )

    return new_user


@router.patch("/{user_id}", response_model=UserRead)
def edit_user(
    user_id: int,
    data: UserUpdate,
    db: Session = Depends(get_session),
):
    user = db.get(User, user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
        )

    update_data = data.model_dump(exclude_unset=True)

    if "password" in update_data and update_data["password"]:
        raw_password = str(update_data.pop("password"))

        update_data["password_hash"] = pwd_context.hash(raw_password)
    elif "password" in update_data:
        update_data.pop("password")

    if "email" in update_data and update_data["email"] != user.email:
        email_exists = db.exec(
            select(User).where(User.email == update_data["email"])
        ).first()

        if email_exists:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Email already in use"
            )

    user.sqlmodel_update(update_data)
    db.add(user)
    db.commit()
    db.refresh(user)

    return user


@router.delete("/{user_id}")
def delete_user(
    user_id: int,
    db: Session = Depends(get_session),
):
    user = db.get(User, user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
        )

    db.delete(user)
    db.commit()

    return {"success": True}


@router.patch("/{user_id}/disable", response_model=UserRead)
def disable_user(
    user_id: int,
    db: Session = Depends(get_session),
):
    user = db.get(User, user_id)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )

    user.is_active = False

    db.add(user)
    db.commit()
    db.refresh(user)

    return user


@router.patch("/{user_id}/enable", response_model=UserRead)
def enable_user(
    user_id: int,
    db: Session = Depends(get_session),
):
    user = db.get(User, user_id)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )

    user.is_active = True

    db.add(user)
    db.commit()
    db.refresh(user)

    return user
