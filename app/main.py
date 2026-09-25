from fastapi import FastAPI, Depends, HTTPException, Response, status
from app.database import create_db_and_tables, get_session, Session
from sqlmodel import select
from app.models import User, UserRead
from pydantic import BaseModel
from datetime import datetime, timedelta, timezone
from utils import pwd_context, get_current_user, SECRET_KEY, ALGORITHM, ONE_MONTH
from user import router as user_router
import jwt

app = FastAPI(title="DineIQ API", version="1.0.0")

app.include_router(user_router)

create_db_and_tables()


class LoginData(BaseModel):
    email: str
    password: str


@app.get("/")
def root():
    return {"message": "DineIQ API is running"}


@app.get("/health")
def check_health():
    return {"message": "api is running healthy!"}


@app.post("/auth/login")
def login(data: LoginData, response: Response, db: Session = Depends(get_session)):
    user = db.exec(select(User).where(User.email == data.email)).first()

    if (
        not user
        or not user.is_active
        or not pwd_context.verify(data.password, user.password_hash)
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
        )

    payload = {
        "sub": str(user.id),
        "exp": datetime.now(timezone.utc) + timedelta(days=30),
    }

    token = jwt.encode(payload, SECRET_KEY, ALGORITHM)

    response.set_cookie(
        "access_token",
        token,
        max_age=ONE_MONTH,
        httponly=True,
        samesite="lax",
    )

    return {"success": True}


@app.get("/auth/me", response_model=UserRead)
def me(
    user: User = Depends(get_current_user),
):
    return user


@app.post("/auth/logout")
def logout(response: Response):
    response.delete_cookie("access_token")
    return {"success": True}
