import logging
import secrets
import sys
from datetime import datetime, timedelta
from urllib.parse import urlencode

import httpx
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from redis import Redis
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api import jobs, tokens
from app.api.deps import get_current_user, require_admin
from app.core.config import get_settings
from app.core.database import engine, get_db, ping_database
from app.core.k8s import lifespan
from app.core.redis import get_redis
from app.core.security import (
    create_access,
    gen_pkce,
    generate_raw_token,
    hash_password,
    hash_token,
    verify_password,
)
from app.models.users import Base, Invitation, SocialIdentity, User
from app.schemas.users import (
    InvitationAcceptIn,
    InvitationVerifyIn,
    InvitationVerifyOut,
    InviteIn,
    LoginIn,
    UserCreate,
    UserOut,
)
from app.services.email_service import send_invitation_via_acs_smtp
from app.services.redis_service import load_oauth_tx, save_oauth_tx

root = logging.getLogger()
if not root.handlers:  # don't double-add in reloads
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    root.addHandler(h)

root.setLevel(logging.INFO)

settings = get_settings()
INVITE_TTL_HOURS = 48

GITHUB_AUTH = "https://github.com/login/oauth/authorize"
GITHUB_TOKEN = "https://github.com/login/oauth/access_token"
GITHUB_USER = "https://api.github.com/user"
GITHUB_EMAILS = "https://api.github.com/user/emails"

CLIENT_ID = settings.github_client_id
CLIENT_SECRET = settings.github_client_secret
REDIRECT_URI = settings.github_redirect_uri
FRONTEND_HOME = settings.frontend_home
SCOPES = "read:user user:email"

JWT_SECRET = settings.jwt_secret
JWT_ALGO = settings.jwt_algo

Base.metadata.create_all(bind=engine)

app = FastAPI(title="walk:ai API", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(jobs.router)
app.include_router(tokens.router)


def _require_base_url() -> str:
    base_url = settings.invite_base_url
    if not base_url:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Invitation service is not configured",
        )
    return base_url.rstrip("/")


def _get_active_invitation(db: Session, token_h: str) -> Invitation | None:
    inv = db.query(Invitation).filter(Invitation.token_hash == token_h).first()
    if not inv:
        return None
    now = datetime.now()
    if inv.used_at is not None or inv.expires_at < now:
        return None
    return inv


def _pick_verified_primary_email(emails: list[dict]) -> str | None:
    for e in emails:
        if e.get("primary") and e.get("verified"):
            return e["email"].strip().lower()
    return None


@app.post("/users", response_model=UserOut, status_code=201)
def register(payload: UserCreate, db: Session = Depends(get_db)):
    existing_user = db.query(User).filter(User.email == payload.email).first()
    if existing_user:
        raise HTTPException(status_code=409, detail="Email already registered")

    user = User(
        email=payload.email.strip().lower(),
        password_hash=hash_password(payload.password),
        role="admin",
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


@app.get("/users", response_model=list[UserOut])
def list_users(db: Session = Depends(get_db)):
    stmt = select(User).order_by(User.id)
    result = db.execute(stmt)
    return result.scalars().unique().all()


@app.post("/login")
def login(payload: LoginIn, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == payload.email).first()
    if (
        not user
        or not user.password_hash
        or not verify_password(payload.password, user.password_hash)
    ):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    access = create_access(str(user.id), user.role)

    resp = JSONResponse({"message": "ok"})
    resp.headers["Cache-Control"] = "no-store"
    resp.set_cookie(
        key="access_token",
        value=access,
        httponly=True,
        secure=False,
        samesite="lax",
        max_age=settings.access_min * 60,
        path="/",
    )
    return resp


@app.post("/admin/invitations", status_code=201)
def create_invitation(
    payload: InviteIn,
    bg: BackgroundTasks,
    db: Session = Depends(get_db),
    current_admin_email: str = Depends(require_admin),
):
    raw_token = generate_raw_token(32)
    token_hash = hash_token(raw_token)
    expires_at = datetime.utcnow() + timedelta(hours=INVITE_TTL_HOURS)

    invitation = Invitation(
        email=payload.email.strip().lower(),
        token_hash=token_hash,
        expires_at=expires_at.replace(microsecond=0),
        invited_by=current_admin_email,
    )

    db.add(invitation)
    db.commit()

    invitation_link = f"{_require_base_url()}?token={raw_token}"
    bg.add_task(send_invitation_via_acs_smtp, payload.email, invitation_link)

    return {"message": "If the email address is valid, instructions will be sent."}


@app.post("/invitations/verify", response_model=InvitationVerifyOut)
def verify_invitation(body: InvitationVerifyIn, db: Session = Depends(get_db)):
    token = body.token.get_secret_value()
    inv = _get_active_invitation(db, hash_token(token))
    if not inv:
        raise HTTPException(status_code=400, detail="Invalid or expired invitation")
    return {"email": inv.email}


@app.post("/invitations/accept", status_code=201)
def accept_invitation(body: InvitationAcceptIn, db: Session = Depends(get_db)):
    token = body.token.get_secret_value()
    token_h = hash_token(token)
    inv = _get_active_invitation(db, token_h)
    if not inv:
        raise HTTPException(status_code=400, detail="Invalid or expired invitation")

    email = inv.email.strip().lower()

    existing = db.query(User).filter(User.email == email).first()
    if existing:
        inv.used_at = datetime.utcnow()
        db.commit()
        raise HTTPException(status_code=409, detail="Account already exists")

    pwd_hash = hash_password(body.password.get_secret_value())
    user = User(email=email, password_hash=pwd_hash, role="user")
    db.add(user)

    inv.used_at = datetime.utcnow()
    db.commit()
    return {"message": "Account created"}


@app.get("/oauth/github/start")
def github_start(
    flow: str,
    invitation_token: str | None = None,
    redis_client: Redis = Depends(get_redis),
):
    if flow == "register" and not invitation_token:
        raise HTTPException(
            status_code=400, detail="invitation_token is required for register"
        )

    code_verifier, code_challenge = gen_pkce()
    state = secrets.token_urlsafe(16)

    data = {"code_verifier": code_verifier, "flow": flow}
    if invitation_token:
        data["invitation_token"] = invitation_token

    save_oauth_tx(redis_client, state, data)

    params = {
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "scope": SCOPES,
        "state": state,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
        "allow_signup": "false",
    }
    return {"authorize_url": f"{GITHUB_AUTH}?{urlencode(params)}"}


@app.get("/oauth/github/callback")
def github_callback(
    code: str,
    state: str,
    db: Session = Depends(get_db),
    redis_client: Redis = Depends(get_redis),
):
    tx = load_oauth_tx(redis_client, state)
    if not tx:
        raise HTTPException(status_code=400, detail="Invalid state")

    inv = None
    if tx.get("flow") == "register":
        inv = (
            db.query(Invitation)
            .filter(Invitation.token_hash == hash_token(tx["invitation_token"]))
            .first()
        )
        if not inv or inv.used_at is not None or inv.expires_at < datetime.utcnow():
            raise HTTPException(status_code=400, detail="Invalid or expired invitation")

    with httpx.Client(timeout=15) as client:
        tok = client.post(
            GITHUB_TOKEN,
            headers={"Accept": "application/json"},
            data={
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "code": code,
                "redirect_uri": REDIRECT_URI,
                "code_verifier": tx["code_verifier"],
            },
        ).json()
    access_token = tok.get("access_token")
    if not access_token:
        raise HTTPException(status_code=400, detail="OAuth exchange failed")

    authz = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    with httpx.Client(timeout=15) as client:
        gh_user = client.get(GITHUB_USER, headers=authz).json()
        gh_emails = client.get(GITHUB_EMAILS, headers=authz).json()

    provider_user_id = str(gh_user["id"])
    gh_email = _pick_verified_primary_email(gh_emails)
    if not gh_email:
        raise HTTPException(status_code=400, detail="GitHub email not verified")

    if tx.get("flow") == "register":
        invited_email = inv.email.strip().lower()
        if gh_email != invited_email:
            raise HTTPException(
                status_code=409, detail="Email mismatch with invitation"
            )

        user = db.query(User).filter(User.email == invited_email).first()
        if not user:
            user = User(email=invited_email, password_hash=None, role="user")
            db.add(user)
            db.flush()

        si = (
            db.query(SocialIdentity)
            .filter(
                SocialIdentity.provider == "github",
                SocialIdentity.provider_user_id == provider_user_id,
            )
            .first()
        )
        if not si:
            db.add(
                SocialIdentity(
                    user_id=user.id,
                    provider="github",
                    provider_user_id=provider_user_id,
                    email_verified=True,
                )
            )

        inv.used_at = datetime.utcnow()
        db.commit()
    else:
        si = (
            db.query(SocialIdentity)
            .filter(
                SocialIdentity.provider == "github",
                SocialIdentity.provider_user_id == provider_user_id,
            )
            .first()
        )
        if not si:
            raise HTTPException(status_code=404, detail="No linked GitHub identity")
        user = db.query(User).filter(User.id == si.user_id).first()

    access = create_access(str(user.id), user.role)

    resp = RedirectResponse(url=FRONTEND_HOME, status_code=303)
    resp.set_cookie(
        "access_token",
        access,
        httponly=True,
        secure=False,
        samesite="Lax",
        max_age=settings.access_min * 60,
        path="/",
    )
    return resp


@app.get("/me", response_model=UserOut)
def me(user: User = Depends(get_current_user)):
    return UserOut.model_validate(user)


@app.post("/logout")
def logout():
    resp = JSONResponse({"message": "ok"})
    resp.headers["Cache-Control"] = "no-store"

    resp.delete_cookie(
        key="access_token",
        path="/",
    )

    return resp


@app.get("/health", tags=["health"])
def health_check():
    """Report service status and confirm database connectivity."""
    database_status = "ok" if ping_database() else "error"
    return {
        "status": "ok",
        "database": database_status,
    }
