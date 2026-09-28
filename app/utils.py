import os
from fastapi import Cookie, Header, Depends, HTTPException, status
from pwdlib import PasswordHash
from sqlmodel import select
from app.database import Session, get_session
from app.models import User, UserRole
import jwt

pwd_context = PasswordHash.recommended()

SECRET_KEY = os.environ["SECRET_KEY"]
ALGORITHM = "HS256"
ONE_MONTH = 60 * 60 * 24 * 30


def user_id_from_access_token(access_token: str | None):
    if not access_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Access token cookie is not set.",
        )

    try:
        decoded_data = jwt.decode(access_token, SECRET_KEY, algorithms=[ALGORITHM])
        return int(decoded_data["sub"])
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


def get_user_with_raise(id: int, db: Session):
    user = db.exec(select(User).where(User.id == id)).first()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User ID not found in database",
        )

    return user


def get_current_user(
    access_token: str | None = Cookie(default=None),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_session),
) -> User:
    if authorization:
        scheme, _, token = authorization.partition(" ")
        if scheme.lower() != "bearer" or not token:
            raise HTTPException(status_code=401, detail="Invalid authorization header")
        access_token = token
    if not access_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
        )

    user_id = user_id_from_access_token(access_token)
    user = get_user_with_raise(user_id, db)

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User account is deactivated",
        )

    return user


def require_admin(current_user: User = Depends(get_current_user)) -> User:
    if current_user.role != UserRole.admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This endpoint requires an admin role",
        )
    return current_user
