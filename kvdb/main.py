from client import Client


if __name__ == '__main__':
    client = Client()
    client.set(1, 10)
    print(client.key_value_pairs())