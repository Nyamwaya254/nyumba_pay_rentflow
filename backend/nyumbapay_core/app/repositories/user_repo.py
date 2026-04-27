"""User repository"""

import uuid
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from nyumbapay_core.app.models.enums import UserRole
from nyumbapay_core.app.models.models import User


class UserRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_id(self, user_id: uuid.UUID) -> User | None:
        """Get a user by the PK"""
        result = await self._session.execute(select(User).where(User.id == user_id))
        return result.scalar_one_or_none()

    async def get_by_clerk_id(self, clerk_user_id: str) -> User | None:
        """Get a user by their clerk_id"""
        result = await self._session.execute(
            select(User).where(User.clerk_user_id == clerk_user_id)
        )
        return result.scalar_one_or_none()

    async def get_by_email(self, email: str) -> User | None:
        """Get a user by their email"""
        result = await self._session.execute(
            select(User).where(User.email == email.lower().strip())
        )
        return result.scalar_one_or_none()

    async def email_exists(self, email: str) -> bool:
        """Check if an email already exists"""
        result = await self._session.execute(
            select(User.id).where(User.email == email.lower().strip())
        )
        return result.scalar_one_or_none() is not None

    async def create(
        self,
        clerk_user_id: str,
        email: str,
        role: UserRole = UserRole.LANDLORD,
    ) -> User:
        """Create a new user"""
        user = User(
            clerk_user_id=clerk_user_id,
            email=email.lower().strip(),
            role=role,
        )
        self._session.add(user)
        await self._session.flush()
        await self._session.refresh(user)
        return user

    async def update_clerk_id(self, user_id: uuid.UUID, clerk_user_id: str) -> None:

        await self._session.execute(
            update(User).where(User.id == user_id).values(clerk_user_id=clerk_user_id)
        )

    async def deactivate(self, user_id: uuid.UUID) -> None:
        await self._session.execute(
            update(User).where(User.id == user_id).values(is_active=False)
        )
