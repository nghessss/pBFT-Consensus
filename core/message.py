from dataclasses import dataclass, field
from typing import Any

@dataclass
class Message:
    type: str              
    term: int
    from_id: int              
    to_id: int              
    payload: dict = field(default_factory=dict)
    direction: str = "request"  
    send_time: float = 0.0
    recv_time: float = 0.0

    def to_reply(self, reply_payload: dict):
        """Create a reply Message swapped sender/receiver and keep type."""
        return Message(
            type=self.type,
            term=reply_payload.get("term", self.term),
            from_id=self.to_id, 
            to_id=self.from_id,
            payload=reply_payload,
            direction="reply"
        )
        
    def __repr__(self):
        return f"<Msg {self.type} {self.from_id}->{self.to_id} t={self.term} dir={self.direction}>"
