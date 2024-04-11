# api_client.py
import requests


# API Endpoint
BASE_URL = "https://1ryu4whyek.execute-api.us-west-2.amazonaws.com/dev/skus"

def create_sku(sku_data):
    try:
        response = requests.post(BASE_URL, json=sku_data)
        response.raise_for_status()
        if response.status_code == 201:
            print("SKU created successfully!")
            return response.json()
        else:
            print("Failed to create SKU:", response.text)
            return None
    except requests.exceptions.RequestException as e:
        print("Error creating SKU:", e)
        return None

def get_all_skus():
    try:
        response = requests.get(BASE_URL)
        response.raise_for_status()
        if response.status_code == 200:
            return response.json()
        else:
            print("Failed to get SKUs:", response.text)
            return None
    except requests.exceptions.RequestException as e:
        print("Error getting SKUs:", e)
        return None

def get_sku_by_id(sku_id):
    try:
        url = f"{BASE_URL}/{sku_id}"
        response = requests.get(url)
        response.raise_for_status()
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to get SKU with ID {sku_id}:", response.text)
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error getting SKU with ID {sku_id}:", e)
        return None

def update_sku(sku_id, sku_data):
    try:
        url = f"{BASE_URL}/{sku_id}"
        response = requests.put(url, json=sku_data)
        response.raise_for_status()
        if response.status_code == 200:
            print("SKU updated successfully!")
            return response.json()
        else:
            print(f"Failed to update SKU with ID {sku_id}:", response.text)
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error updating SKU with ID {sku_id}:", e)
        return None

def delete_sku(sku_id):
    try:
        url = f"{BASE_URL}/{sku_id}"
        response = requests.delete(url)
        response.raise_for_status()
        if response.status_code == 204:
            print("SKU deleted successfully!")
        else:
            print(f"Failed to delete SKU with ID {sku_id}:", response.text)
    except requests.exceptions.RequestException as e:
        print(f"Error deleting SKU with ID {sku_id}:", e)

# Example usage
if __name__ == "__main__":
    # Create a new SKU
    new_sku_data = {
        "sku": "berliner",
        "description": "Jelly donut",
        "price": "2.99"
    }
    created_sku = create_sku(new_sku_data)
    if created_sku:
        sku_id = created_sku['id']
        
        # Get all SKUs
        all_skus = get_all_skus()
        print("All SKUs:", all_skus)
        
        # Get SKU by ID
        sku_by_id = get_sku_by_id(sku_id)
        print(f"SKU with ID {sku_id}:", sku_by_id)
        
        # Update SKU
        updated_sku_data = {
            "sku": "berliner",
            "description": "Chocolate donut",
            "price": "3.49"
        }
        updated_sku = update_sku(sku_id, updated_sku_data)
        print("Updated SKU:", updated_sku)
        
        # Delete SKU
        delete_sku(sku_id)

