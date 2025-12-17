def test_admin_can_update_user_quota(auth_client, db_session):
    client, user = auth_client

    response = client.patch(
        f"/admin/users/{user.id}/quota",
        json={"high_priority_quota_minutes": 60},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["id"] == user.id
    assert data["high_priority_quota_minutes"] == 60

    db_session.refresh(user)
    assert user.high_priority_quota_minutes == 60
