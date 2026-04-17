from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel

fake_users_db = {
    "operator": {"username": "operator", "password": "operator", "role": "operator"},
    "supervisor": {"username": "supervisor", "password": "supervisor", "role": "supervisor"},
    "admin": {"username": "admin", "password": "admin", "role": "admin"},
}

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

def get_user(token: str):
    user = fake_users_db.get(token)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user