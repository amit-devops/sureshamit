def test_controller_health_check_down(mocker, work_load_client):
    mocker.patch(
        "app.common.views.health.check_local_celery_worker",
        return_value="Workers are Down",
    )
    mocker.patch(
        "app.common.views.health.revoke_and_terminate_running_tasks",
        return_value=[],
    )
    health_response = work_load_client.get("/api/health/check/")
    non_health_response = work_load_client.get("/api/not_health/")
    for response in [health_response, non_health_response]:
        assert response.status_code == 500
        assert response.json["status"] == "DOWN"


def test_controller_health_check_up(mocker, work_load_client):
    mocker.patch("app.common.views.health.get_app_status", return_value="UP")
    mocker.patch(
        "app.common.views.health.check_local_celery_worker", return_value=""
    )
    response = work_load_client.get("/api/health/check/")
    assert response.status_code == 200
    assert response.json["status"] == "UP"


def test_controller_health_down(mocker, work_load_client):
    mocker.patch("app.common.views.health.get_app_status", return_value="DOWN")
    mocker.patch(
        "app.common.views.health.stop_local_celery_worker_listening",
        return_value=" ",
    )
    mocker.patch(
        "app.common.views.health.revoke_and_terminate_running_tasks",
        return_value=[],
    )
    set_status = mocker.patch("app.common.views.health.set_app_status")
    work_load_client.get("/api/health/down/")
    set_status.assert_called_with("DOWN")


def test_denormalization_health_check_down(mocker, denormalization_client):
    mocker.patch(
        "app.common.views.health.check_local_celery_worker",
        return_value="Workers are Down",
    )
    mocker.patch(
        "app.common.views.health.revoke_and_terminate_running_tasks",
        return_value=[],
    )
    response = denormalization_client.get("/api/health/check/")
    assert response.status_code == 500
    assert response.json["status"] == "DOWN"


def test_denormalization_health_check_up(mocker, denormalization_client):
    mocker.patch("app.common.views.health.get_app_status", return_value="UP")
    mocker.patch(
        "app.common.views.health.check_local_celery_worker", return_value=""
    )
    response = denormalization_client.get("/api/health/check/")
    assert response.status_code == 200
    assert response.json["status"] == "UP"


def test_prediction_health_check_down(mocker, prediction_client):
    mocker.patch(
        "app.common.views.health.check_local_celery_worker",
        return_value="Workers are Down",
    )
    mocker.patch(
        "app.common.views.health.revoke_and_terminate_running_tasks",
        return_value=[],
    )
    response = prediction_client.get("/api/health/check/")
    assert response.status_code == 500
    assert response.json["status"] == "DOWN"


def test_prediction_health_check_up(mocker, prediction_client):
    mocker.patch("app.common.views.health.get_app_status", return_value="UP")
    mocker.patch(
        "app.common.views.health.check_local_celery_worker", return_value=""
    )
    response = prediction_client.get("/api/health/check/")
    assert response.status_code == 200
    assert response.json["status"] == "UP"
