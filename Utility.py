import time
import uuid


class Utility:

    @staticmethod
    def current_timestamp():
        return int(time.time())

    @staticmethod
    def my_random_string(string_length=10):
        random = str(uuid.uuid4())
        random = random.replace("-", "")
        return random[0:string_length]