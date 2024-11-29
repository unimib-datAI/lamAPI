from locust import HttpUser, task, between

class LamApiUser(HttpUser):
    # Simulate a wait time between requests
    wait_time = between(1, 3)

    @task
    def test_entity_retrieval(self):
        # Send GET requests to the lamAPI endpoint
        self.client.get(
            "/lookup/entity-retrieval",
            params={
                "name": "Batman Begins",
                "limit": 10,
                "token": "lamapi_demo_2023"
            },
            headers={"accept": "application/json"}
        )