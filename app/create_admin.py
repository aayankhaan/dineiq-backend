"""Create the first administrator without hard-coded credentials.

Run from the project root: python -m app.create_admin
"""

from getpass import getpass
from sqlmodel import Session, select
from app.database import engine, create_db_and_tables
from app.models import User, UserRole
from app.utils import pwd_context


def main():
    create_db_and_tables()
    with Session(engine) as db:
        if db.exec(select(User).where(User.role == UserRole.admin)).first():
            raise SystemExit(
                "An administrator already exists. Use the Admin page to create accounts."
            )
        name = input("Name: ").strip()
        email = input("Email: ").strip()
        password = getpass("Password (at least 8 characters): ")
        if not name or "@" not in email or len(password) < 8:
            raise SystemExit(
                "Enter a name, email and a password of at least 8 characters."
            )
        if password != getpass("Confirm password: "):
            raise SystemExit("Passwords do not match.")
        if db.exec(select(User).where(User.email == email)).first():
            raise SystemExit("That email is already registered.")
        db.add(
            User(
                name=name,
                email=email,
                password_hash=pwd_context.hash(password),
                role=UserRole.admin,
            )
        )
        db.commit()
    print("Administrator created. Sign in using this email and password.")


if __name__ == "__main__":
    main()
