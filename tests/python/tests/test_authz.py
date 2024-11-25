import sys
import uuid

import requests

import conftest
import pytest


def create_user():
    from keycloak import KeycloakOpenIDConnection, KeycloakAdmin
    # TODO: dont use OPENID_PROVIDER_URI here
    keycloak_connection = KeycloakOpenIDConnection(server_url="http://keycloak:8080",
                                                   username="admin",
                                                   password="admin",
                                                   realm_name="master",
                                                   verify=True)
    keycloak_connection.get_token()
    token = keycloak_connection.token
    client_id = f"test-{uuid.uuid4()}"
    keycloak_admin = KeycloakAdmin(server_url=conftest.OPENID_PROVIDER_URI.rstrip("realms/test"), token=token,
                                   verify=True, realm_name="test")
    client_representation = {
        "enabled": True,
        "protocol": "openid-connect",
        "publicClient": False,
        "directAccessGrantsEnabled": True,
        "serviceAccountsEnabled": True,
        "clientId": client_id,
    }

    user = keycloak_admin.create_client(client_representation)
    secret = keycloak_admin.get_client_secrets(user)
    return client_id, secret["value"]


def get_token(client_id: str, secret: str) -> str:
    from keycloak import KeycloakOpenIDConnection
    keycloak_connection = KeycloakOpenIDConnection(server_url=conftest.OPENID_PROVIDER_URI.rstrip("realms/test"),
                                                   client_id=client_id,
                                                   client_secret_key=secret,
                                                   realm_name="test",
                                                   verify=True)
    keycloak_connection.get_token()
    return keycloak_connection.token['access_token']


def test_provision_user(server: conftest.Server, access_token: str):
    client_id, secret = create_user()
    resp = requests.post(server.management_url + "v1/user", json={
        "id": f"{client_id}",
        "name": f"{client_id}",
        "user-type": "application"
    }, headers={"Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json"})
    resp.raise_for_status()
    assert resp.status_code == 201
    parsed_user = resp.json()
    assert parsed_user["id"] == client_id

    user = requests.get(server.management_url + f"v1/user/{parsed_user['id']}",
                        headers={"Authorization": f"Bearer {access_token}"})
    user.raise_for_status()
    assert user.status_code == 200
    parsed_user = user.json()
    print(parsed_user, file=sys.stderr)
    assert parsed_user["id"] == client_id
    assert parsed_user["user-type"] == "application"
    assert parsed_user["name"] == client_id

    new_user_token = get_token(client_id, secret)
    whoami = requests.get(server.management_url + "v1/whoami", headers={"Authorization": f"Bearer {new_user_token}"})
    whoami.raise_for_status()
    parsed_whoami = whoami.json()
    assert whoami.status_code == 200
    assert parsed_whoami["id"] == client_id
    assert parsed_whoami["user-type"] == "application"
