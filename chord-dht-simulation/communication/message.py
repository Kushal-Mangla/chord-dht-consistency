class MessageType:
    PUT = "PUT"
    GET = "GET"
    PUT_REPLY = "PUT_REPLY"
    GET_REPLY = "GET_REPLY"
    ERROR = "ERROR"
    GET_RING_INFO = "GET_RING_INFO"
    GET_RING_INFO_REPLY = "GET_RING_INFO_REPLY"
    GET_ALL_KEYS = "GET_ALL_KEYS"
    GET_ALL_KEYS_REPLY = "GET_ALL_KEYS_REPLY"

class Message:
    def __init__(self, msg_type: str, sender_id: int, sender_address: str, msg_id: int, data: dict):
        self.msg_type = msg_type
        self.sender_id = sender_id
        self.sender_address = sender_address
        self.msg_id = msg_id
        self.data = data

    def to_dict(self):
        return {
            "msg_type": self.msg_type,
            "sender_id": self.sender_id,
            "sender_address": self.sender_address,
            "msg_id": self.msg_id,
            "data": self.data
        }

    @staticmethod
    def from_dict(data: dict):
        return Message(
            msg_type=data["msg_type"],
            sender_id=data["sender_id"],
            sender_address=data["sender_address"],
            msg_id=data["msg_id"],
            data=data["data"]
        )