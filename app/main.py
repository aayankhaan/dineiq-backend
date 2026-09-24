from fastapi import Cookie, FastAPI, Depends, HTTPException, Response, status
from app.database import create_db_and_tables, get_session, Session
from sqlmodel import select
from app.models import User, UserRead
from pydantic import BaseModel
from pwdlib import PasswordHash
from datetime import datetime, timedelta, timezone
import jwt

app = FastAPI(title="DineIQ API", version="1.0.0")

pwd_context = PasswordHash.recommended()

SECRET_KEY = "secret-key-key-key-873y458735-iuefngdfg"
ALGORITHM = "HS256"
ONE_MONTH = 60 * 60 * 24 * 30

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

    if not user or not pwd_context.verify(data.password, user.password_hash):
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
    access_token: str | None = Cookie(default=None), db: Session = Depends(get_session)
):
    if not access_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Access token cookie is not set.",
        )

    try:
        decoded_data = jwt.decode(access_token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = int(decoded_data["sub"])
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Access token has expired, please login again.",
        )
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Access token is invalid.",
        )

    user = db.exec(select(User).where(User.id == user_id)).first()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User ID not found in database",
        )

    return user


@app.post("/auth/logout")
def logout(response: Response):
    response.delete_cookie("access_token")
    return {"success": True}
