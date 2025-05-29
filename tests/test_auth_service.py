import pytest
from label_pizza.services import AuthService
import pandas as pd

def test_auth_service_create_user(session):
    """Test creating a new user."""
    user = AuthService.create_user(
        user_id="test_user",
        email="test@example.com",
        password_hash="test_hash",
        user_type="admin",
        session=session
    )
    assert user.user_id_str == "test_user"
    assert user.email == "test@example.com"
    assert user.password_hash == "test_hash"
    assert user.user_type == "admin"
    assert not user.is_archived

def test_auth_service_create_user_duplicate_id(session, test_user):
    """Test creating a user with duplicate user ID."""
    with pytest.raises(ValueError, match="already exists"):
        AuthService.create_user(
            user_id="test_user",
            email="other@example.com",
            password_hash="test_hash",
            user_type="admin",
            session=session
        )

def test_auth_service_create_user_duplicate_email(session, test_user):
    """Test creating a user with duplicate email."""
    with pytest.raises(ValueError, match="already exists"):
        AuthService.create_user(
            user_id="other_user",
            email="test@example.com",
            password_hash="test_hash",
            user_type="admin",
            session=session
        )

def test_auth_service_create_user_invalid_type(session):
    """Test creating a user with invalid type."""
    with pytest.raises(ValueError, match="Invalid user type"):
        AuthService.create_user(
            user_id="test_user",
            email="test@example.com",
            password_hash="test_hash",
            user_type="invalid",
            session=session
        )

def test_auth_service_get_user_by_id(session, test_user):
    """Test getting a user by ID."""
    user = AuthService.get_user_by_id("test_user", session)
    assert user is not None
    assert user.id == test_user.id
    assert user.user_id_str == "test_user"
    assert user.email == "test@example.com"

def test_auth_service_get_user_by_id_not_found(session):
    """Test getting a non-existent user by ID."""
    user = AuthService.get_user_by_id("non_existent_user", session)
    assert user is None

def test_auth_service_get_user_by_id_archived(session, test_user):
    """Test getting an archived user by ID."""
    AuthService.archive_user(test_user.id, session)
    with pytest.raises(ValueError, match="User not found"):
        AuthService.get_user_by_id("test_user", session)

def test_auth_service_get_user_by_email(session, test_user):
    """Test getting a user by email."""
    user = AuthService.get_user_by_email("test@example.com", session)
    assert user.id == test_user.id
    assert user.user_id_str == "test_user"
    assert user.email == "test@example.com"

def test_auth_service_get_user_by_email_not_found(session):
    """Test getting a non-existent user by email."""
    with pytest.raises(ValueError, match="User not found"):
        AuthService.get_user_by_email("non_existent@example.com", session)

def test_auth_service_get_user_by_email_archived(session, test_user):
    """Test getting an archived user by email."""
    AuthService.archive_user(test_user.id, session)
    with pytest.raises(ValueError, match="User not found"):
        AuthService.get_user_by_email("test@example.com", session)

def test_auth_service_archive_user(session, test_user):
    """Test archiving a user."""
    AuthService.archive_user(test_user.id, session)
    user = AuthService.get_user_by_id("test_user", session, include_archived=True)
    assert user.is_archived

def test_auth_service_archive_user_not_found(session):
    """Test archiving a non-existent user."""
    with pytest.raises(ValueError, match="User not found"):
        AuthService.archive_user(999, session)

def test_auth_service_get_user_by_id_include_archived(session, test_user):
    """Test getting a user by ID including archived ones."""
    AuthService.archive_user(test_user.id, session)
    user = AuthService.get_user_by_id("test_user", session, include_archived=True)
    assert user.id == test_user.id
    assert user.is_archived

def test_auth_service_get_user_by_email_include_archived(session, test_user):
    """Test getting a user by email including archived ones."""
    AuthService.archive_user(test_user.id, session)
    user = AuthService.get_user_by_email("test@example.com", session, include_archived=True)
    assert user.id == test_user.id
    assert user.is_archived

def test_auth_service_get_all_users(session, test_user):
    """Test getting all users."""
    df = AuthService.get_all_users(session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["ID"] == test_user.id
    assert df.iloc[0]["User ID"] == "test_user"
    assert df.iloc[0]["Email"] == "test@example.com"
    assert df.iloc[0]["Role"] == "admin"
    assert not df.iloc[0]["Archived"]

def test_auth_service_get_all_users_empty(session):
    """Test getting all users when none exist."""
    df = AuthService.get_all_users(session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0

def test_auth_service_get_all_users_with_archived(session, test_user):
    """Test getting all users including archived ones."""
    AuthService.archive_user(test_user.id, session)
    users = AuthService.get_all_users(session, include_archived=True)
    assert len(users) == 1
    assert users[0].id == test_user.id
    assert users[0].is_archived

def test_auth_service_get_users_by_type(session, test_user):
    """Test getting users by type."""
    users = AuthService.get_users_by_type("admin", session)
    assert len(users) == 1
    assert users[0].id == test_user.id
    assert users[0].user_type == "admin"

def test_auth_service_get_users_by_type_empty(session):
    """Test getting users by type when none exist."""
    users = AuthService.get_users_by_type("admin", session)
    assert len(users) == 0

def test_auth_service_get_users_by_type_with_archived(session, test_user):
    """Test getting users by type including archived ones."""
    AuthService.archive_user(test_user.id, session)
    users = AuthService.get_users_by_type("admin", session, include_archived=True)
    assert len(users) == 1
    assert users[0].id == test_user.id
    assert users[0].is_archived

def test_auth_service_update_user_id(session, test_user):
    """Test updating a user's ID."""
    AuthService.update_user_id(test_user.id, "new_user_id", session)
    user = AuthService.get_user_by_id("new_user_id", session)
    assert user is not None
    assert user.id == test_user.id
    assert user.user_id_str == "new_user_id"

def test_auth_service_update_user_id_duplicate(session, test_user):
    """Test updating a user's ID to one that already exists."""
    # Create another user
    AuthService.create_user(
        user_id="other_user",
        email="other@example.com",
        password_hash="test_hash",
        user_type="admin",
        session=session
    )
    with pytest.raises(ValueError, match="already exists"):
        AuthService.update_user_id(test_user.id, "other_user", session)

def test_auth_service_update_user_email(session, test_user):
    """Test updating a user's email."""
    AuthService.update_user_email(test_user.id, "new@example.com", session)
    user = AuthService.get_user_by_id("test_user", session)
    assert user is not None
    assert user.email == "new@example.com"

def test_auth_service_update_user_email_duplicate(session, test_user):
    """Test updating a user's email to one that already exists."""
    # Create another user
    AuthService.create_user(
        user_id="other_user",
        email="other@example.com",
        password_hash="test_hash",
        user_type="admin",
        session=session
    )
    with pytest.raises(ValueError, match="already exists"):
        AuthService.update_user_email(test_user.id, "other@example.com", session)

def test_auth_service_update_user_password(session, test_user):
    """Test updating a user's password."""
    AuthService.update_user_password(test_user.id, "new_password", session)
    user = AuthService.get_user_by_id("test_user", session)
    assert user is not None
    assert user.password_hash == "new_password"
    assert user.password_updated_at is not None

def test_auth_service_update_user_role(session, test_user):
    """Test updating a user's role."""
    AuthService.update_user_role(test_user.id, "human", session)
    user = AuthService.get_user_by_id("test_user", session)
    assert user is not None
    assert user.user_type == "human"

def test_auth_service_update_user_role_invalid(session, test_user):
    """Test updating a user's role to an invalid value."""
    with pytest.raises(ValueError, match="Invalid role"):
        AuthService.update_user_role(test_user.id, "invalid", session)

def test_auth_service_toggle_user_archived(session, test_user):
    """Test toggling a user's archived status."""
    # Archive user
    AuthService.toggle_user_archived(test_user.id, session)
    user = AuthService.get_user_by_id("test_user", session)
    assert user is None  # Archived users are not returned by default
    
    # Unarchive user
    AuthService.toggle_user_archived(test_user.id, session)
    user = AuthService.get_user_by_id("test_user", session)
    assert user is not None
    assert not user.is_archived

def test_auth_service_get_project_assignments(session, test_user):
    """Test getting project assignments."""
    df = AuthService.get_project_assignments(session)
    assert isinstance(df, pd.DataFrame)
    # Add more specific assertions based on your test data setup 