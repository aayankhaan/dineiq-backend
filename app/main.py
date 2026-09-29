from fastapi import FastAPI, Depends, HTTPException, Response, status
from app.database import create_db_and_tables, get_session, Session
from sqlmodel import select
from app.models import User, UserRead
from pydantic import BaseModel
from datetime import datetime, timedelta, timezone
from app.utils import pwd_context, get_current_user, SECRET_KEY, ALGORITHM, ONE_MONTH
from app.user import router as user_router
from app.analytics_api import router as analytics_router, log
from contextlib import asynccontextmanager
from sqlalchemy.exc import SQLAlchemyError
from fastapi.responses import JSONResponse
import jwt


@asynccontextmanager
async def lifespan(app):
    create_db_and_tables()
    yield


app = FastAPI(title="DineIQ API", version="1.0.0", lifespan=lifespan)


@app.exception_handler(SQLAlchemyError)
async def database_error(request, exc):
    return JSONResponse(
        status_code=503,
        content={
            "detail": "The database is unavailable. Check the database connection and try again."
        },
    )


app.include_router(user_router)
app.include_router(user_router, prefix="/api")
app.include_router(analytics_router, prefix="/api")


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
@app.post("/api/auth/login")
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
    log(db, user, "Authentication", "Signed in")

    response.set_cookie(
        "access_token",
        token,
        max_age=ONE_MONTH,
        httponly=True,
        samesite="lax",
    )

    return {
        "success": True,
        "token": token,
        "must_change_password": user.must_change_password,
        "user": {
            "id": user.id,
            "name": user.name,
            "username": user.email,
            "role": "manager"
            if user.role.value == "restaurant_manager"
            else user.role.value,
        },
    }


@app.get("/auth/me", response_model=UserRead)
@app.get("/api/auth/me", response_model=UserRead)
def me(
    user: User = Depends(get_current_user),
):
    return user


@app.post("/auth/logout")
@app.post("/api/auth/logout")
def logout(response: Response):
    response.delete_cookie("access_token")
    return {"success": True}

class ChangePasswordData(BaseModel):
    current_password: str
    new_password: str

@app.post("/auth/change-password")
@app.post("/api/auth/change-password")
def change_password(
    data: ChangePasswordData,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_session)
):
    if not pwd_context.verify(data.current_password, user.password_hash):
        raise HTTPException(status_code=400, detail="Current password is incorrect")
    if len(data.new_password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters")

    user.password_hash = pwd_context.hash(data.new_password)
    user.must_change_password = False
    db.add(user)
    db.commit()
    log(db, user, "Authentication", "Changed password")
    return {"success": True}
