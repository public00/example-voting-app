from app import app

def test_home_route():
    client = app.test_client()
    
    response = client.get('/')
    
    # Assert that it worked (200 OK)
    assert response.status_code == 200
    assert b"Cats" in response.data
    assert b"Dogs" in response.data